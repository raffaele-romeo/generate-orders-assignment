import java.io.{BufferedWriter, File, FileWriter}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util.UUID

import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.implicits._
import org.scalacheck.rng.Seed
import org.scalacheck.{Arbitrary, Gen}
import cats.effect.{Async, IO, Resource, Timer}
import io.circe.generic.AutoDerivation
import io.circe.generic.auto._
import io.circe.syntax._
import JustEatExercise._
import fs2.{Pipe, Stream}
import fs2.concurrent.Queue

import scala.concurrent.duration._
import scala.util.Random

sealed trait EventType
object EventType {

  case object OrderPlaced extends EventType

  case object OrderDelivered extends EventType

  case object OrderCancelled extends EventType

  case class Event(`type`: EventType, data: Data) extends AutoDerivation

  case class Data(orderId: UUID, timestampUtc: LocalDateTime)
    extends AutoDerivation

}

object JustEatExerciseMain extends App {

  val outputPath                           = System.getProperty("java.io.tmpdir") + File.separator + "Interview-Raffaele" + File.separator
  val numberOrders                         = 1000000
  val batchSize                            = 5000
  val numberOfBatches                      = Math.ceil(numberOrders.toDouble / batchSize).toInt
  val intervalFileCreation: FiniteDuration = 1.seconds

  new File(outputPath).mkdir()
  println("Output Path is: " + outputPath)

  val executionContext              = scala.concurrent.ExecutionContext.global
  implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)
  implicit val timer: Timer[IO]     = IO.timer(executionContext)

  val controlFlowStream: Stream[IO, FiniteDuration] =
    Stream.awakeEvery[IO](intervalFileCreation)

  //Pipeline way 1
  val pipeline = Stream
    .range(0, numberOfBatches)
    .evalMap{ _ => IO.delay(generateEvents(batchSize).map(_.asJson.noSpaces))}
    .evalMap(seq =>
      writeToFile[IO](new File(outputPath + getFileName), seq))
    .zip(controlFlowStream)

  //Pipeline way 2
  //  val pipeline = Stream
  //    .fromIterator[IO](
  //      generateEvents(numberOrders)
  //        .map(_.asJson.noSpaces)
  //        .grouped(batchSize))
  //    .evalMap(seq =>
  //      writeToFile[IO](new File(outputPath + getFileName), seq))
  //    .zip(controlFlowStream)

  //Pipeline way 3
  //  val pipeline: Stream[IO, (Unit, FiniteDuration)] = Stream
  //    .range(0, numberOfBatches)
  //    .map { i =>
  //      generatesToQueue[IO, Int](i,
  //                                batchSize,
  //                                new File(outputPath + getFileName))
  //    }
  //    .parJoin(3)
  //    .zip(controlFlowStream)

  pipeline.compile.drain.unsafeRunSync()
}

object JustEatExercise {

  import EventType._
  private def defaultSeed: Long = new Random().nextLong()

  def generatesToQueue[F[_], A](
                                 iteratorNumber: Int,
                                 batchSize: Int,
                                 destination: File)(implicit F: Concurrent[F]): Stream[F, Unit] = {
    Stream.eval(Queue.unbounded[F, String]).flatMap { q =>
      val data =
        Stream(generateEvents(batchSize).map(_.asJson.noSpaces): _*)
          .covary[F]
          .through(q.enqueue)

      val toFile =
        q.dequeue.through(log(s"$iteratorNumber write now")).drain

      toFile mergeHaltBoth data
    }
  }

  def log[F[_], A](prefix: String)(
    implicit F: Concurrent[F]): Pipe[F, A, A] = _.evalMap { a =>
    F.delay { println(s"$prefix> $a"); a }
  }

  def writeToFile[F[_]: Concurrent](destination: File,
                                    lines: Seq[String]): F[Unit] = {
    for {
      guard <- Semaphore[F](1)
      _ <- outputStream(destination, guard).use { f =>
        guard.withPermit(transfer(f, lines))
      }
    } yield Unit
  }

  def transfer[F[_]: Async](destination: BufferedWriter,
                            lines: Seq[String]): F[Unit] = {
    Async[F].delay {
      for (line <- lines) {
        destination.write(line + "\n")
      }
    }
  }

  def outputStream[F[_]: Async](
                                 f: File,
                                 guard: Semaphore[F]): Resource[F, BufferedWriter] = {
    Resource.make {
      Async[F].delay(new BufferedWriter(new FileWriter(f)))
    } { outStream =>
      guard.withPermit(
        Async[F].delay(outStream.close()).handleError(_ => Sync[F].unit)
      )
    }
  }

  def getFileName =
    s"orders-${DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-SSSSSS").format(LocalDateTime.now)}.json"

  def generateEvents(numberOfRecords: Int): Seq[Event] = {
    val getEvent: Long => Arbitrary[Event] = (id: Long) =>
      Arbitrary {
        for {
          eventType <- if (id % 5 == 0) Gen.const(OrderCancelled)
          else Gen.const(OrderDelivered)
          orderId <- Gen.const(UUID.randomUUID())
          timestamp <- Gen.const(
            ZonedDateTime.now
              .withZoneSameInstant(ZoneId.of("UTC"))
              .toLocalDateTime)
        } yield Event(eventType, Data(orderId, timestamp))
      }

    randomWithIds[Event](getEvent, numberOfRecords).flatMap(
      event =>
        List(
          event.copy(`type` = OrderPlaced),
          event
        ))
  }

  def randomWithIds[T](buildArbitrary: Long => Arbitrary[T],
                       numberOfRecords: Int,
                       seed: Long = defaultSeed): Seq[T] = {
    val ids = 1L.to(numberOfRecords, 1)
    val gens =
      ids.map(id => Gen.const(id).map(buildArbitrary).flatMap(_.arbitrary))
    val gen = Gen.sequence[Seq[T], T](gens)
    gen(
      Gen.Parameters.default.withSize(numberOfRecords),
      Seed.apply(seed)
    ).toStream.flatten
  }
}
