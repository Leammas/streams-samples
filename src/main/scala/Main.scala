import ActorSink.{Ackn, Complete, InitComplete, Reply}
import Main.Event
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import fs2.Stream
import fs2.util.{Attempt, Catchable}
import monix.eval.Coeval.{Error, Now}
import monix.eval.{Callback, Task}
import monix.execution.Ack.{Continue, Stop}
import monix.execution.Scheduler.Implicits.global
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive.OverflowStrategy.{BackPressure, DropNewAndSignal}
import monix.reactive.observers.{BufferedSubscriber, Subscriber}
import monix.reactive.{Consumer, Observable}
import swave.core.{Drain, Spout, StreamEnv}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success}

object Main extends App {

  case class Event(id: Long, data: String)

  val interval = 200 millis

  def saveEventF(e: Event)(implicit ec: ExecutionContext): Future[Int] = Future {
    Thread.sleep(600)
    val id = Random.nextInt(100)
    println(s"$id run saving $e")
    id
  }

  /* Monix */

  val evts = Observable.interval(interval)
    .map(Event(_, "foo"))
    .take(10)

  def fromFuture[T](f: ExecutionContext => Future[T]): Task[T] = {
    val t = Task.create[T] { (sch, cb) =>
      f(sch).onComplete({
        case Success(value) =>
          cb.onSuccess(value)
        case Failure(ex) =>
          cb.onError(ex)
      })(sch)

      // Scala Futures are not cancelable, so
      // we shouldn't pretend that they are!
      Cancelable.empty
    }
    t.memoize
  }

  def saveEventT(e: Event): Task[Int] = fromFuture { implicit ec =>
    saveEventF(e)
  }

  val saverFold = Consumer.foldLeftAsync[List[Int], Event](List.empty[Int]) { (z, a) =>
    saveEventT(a).map( z :+ _)
  }

  val saverFoldBuffer =  new Consumer[Event, List[Int]] {
    def createSubscriber(cb: Callback[List[Int]], s: Scheduler): (Subscriber[Event], AssignableCancelable) = {
      val (subsc, cancel) = saverFold.createSubscriber(cb, s)
      BufferedSubscriber[Event](subsc, BackPressure(6)) -> cancel // somehow silently drops one or more events
    }
  }

  val saverByHand = new BufferedSaver

  val loadBalancer = {
    Consumer
      .loadBalance(parallelism=10, saverByHand)
      .map(_.flatten)
  }

  val task: Task[List[Int]] = evts.consumeWith(saverFold)

  task.runAsync.foreach(println)

  /* Akka streams */

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()

  val src = Source.tick(0 second, interval, 1L).take(10)
  val evtF = (x: Long) => Event(x, "bar")
  val f = Flow.fromFunction(evtF)

  val ref = system.actorOf(Props[ActorSink], "sink")
  val sinkByHand = Sink.actorRefWithAck[Event](ref, InitComplete, Ackn, Complete)
  val sinkFold = Sink.foldAsync[List[Int], Event](List.empty[Int]) { (z, a) =>
    saveEventF(a).map( z :+ _)
  }

  val graph = src.via(f).to(sinkByHand)
  graph.run()//.foreach(println)

  /* Swave */

  implicit val env = StreamEnv()

  val spout = Spout.tick(1, interval).map(Event(_, "baz")).take(10)
  val drainFold = Drain.fold[Event, Future[List[Int]]](Future.successful(List.empty[Int])) { (z, a) =>
    for {
      z <- z
      id <- saveEventF(a)
    } yield z :+ id
  }

  spout.to(drainFold).run.result.map(x => x.foreach(println))

  /* FS2 */

  implicit val fc = new Catchable[Future] {
    def fail[A](err: Throwable): Future[A] = Future.failed(err)

    def attempt[A](fa: Future[A]): Future[Attempt[A]] = {
      val p = Promise[Attempt[A]]
      fa.onComplete {
        case Success(x) => p.success(Right(x))
        case Failure(ex) => p.success(Left(ex))
      }
      p.future
    }

    def flatMap[A, B](a: Future[A])(f: (A) => Future[B]): Future[B] = a.flatMap(f)

    def pure[A](a: A): Future[A] = Future.successful(a)
  }

  val stream = Stream.eval(Future {
    Thread.sleep(200)
    1
  }).repeat.map(Event(_, "bam")).take(10)

  implicit val S = fs2.Strategy.fromExecutionContext(global)

  val cup = stream.flatMap(x => Stream.eval(saveEventF(x))).fold(List.empty[Int]) { (z, a) =>
    z :+ a
  }

  cup.runLast.foreach(println)

  Thread.sleep(10000)
  ref ! PoisonPill
  system.terminate()
  env.shutdown()

}

class ActorSink extends Actor {
  var processingCnt = 0
  var ids = ListBuffer.empty[Int]

  def receive: Receive = {
    case InitComplete => sender() ! Ackn
    case e: Event =>
      val sdr = sender()
      processingCnt = processingCnt + 1
      Main.saveEventF(e).map(Reply(sdr, _)).pipeTo(self)
    case Reply(snd, i) =>
      ids += i
      snd ! Ackn
    case Complete =>
      context.become(completing)
  }

  def completing: Receive = {
    case Reply(snd, i) =>
      ids += i
      snd ! Ackn
      if (ids.length == processingCnt) {
        println(ids)
        self ! PoisonPill
      }
  }
}

object ActorSink {
  case object InitComplete
  case object Ackn
  case object Complete
  case class Reply(ref: ActorRef, i: Int)
}

class BufferedSaver extends Consumer[Event, List[Int]] {
  def createSubscriber(cb: Callback[List[Int]], s: Scheduler): (Subscriber[Event], AssignableCancelable) = {
    val out = new Subscriber[Event] {
      implicit def scheduler: Scheduler = s
      private[this] val ids = ListBuffer.empty[Int]
      private[this] var isDone = false

      def onNext(elem: Event): Future[Ack] = {
        // Protects calls to user code from within the operator,
        // as a matter of contract.
        try {
          val t = Main.saveEventT(elem).materializeAttempt.map {
            case Now(id) =>
              ids += id
              Continue
            case Error(ex) =>
              onError(ex)
              Stop
          }
          t.runAsync(s)
        }
        catch {
          case NonFatal(ex) =>
            onError(ex)
            Stop
        }
      }

      def onError(ex: Throwable): Unit = if (!isDone) {
        isDone = true
        cb.onError(ex)
      }

      def onComplete(): Unit = if (!isDone) {
        isDone = true
        cb.onSuccess(ids.toList)
      }
    }

    BufferedSubscriber[Event](out, DropNewAndSignal(6, x => {
      println(s"$x is dropped")
      None})) -> AssignableCancelable.dummy
  }
}