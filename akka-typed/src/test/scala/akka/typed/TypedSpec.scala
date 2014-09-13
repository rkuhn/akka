package akka.typed

import org.scalatest.Spec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import akka.actor.ActorSystem
import akka.testkit.AkkaSpec
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.Future

class TypedSpec extends Spec with Matchers with BeforeAndAfterAll {

  implicit val system = ActorSystem(AkkaSpec.getCallerName(classOf[TypedSpec]), AkkaSpec.testConf)

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 10.seconds)
  }

  def await[T](f: Future[T]): T = Await.result(f, 10.seconds)

}