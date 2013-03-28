package docs.io.japi;

import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.io.HasActorContext;
import akka.io.PipelineFactory;
import akka.io.PipelineInjector;
import akka.io.PipelineSink;
import akka.io.PipelineStage;
import akka.io.TickGenerator;
import akka.util.ByteString;
import scala.concurrent.duration.*;

public class Processor extends UntypedActor {

  private class Context implements HasByteOrder, HasActorContext {

    @Override
    public ActorContext context() {
      return getContext();
    }

    @Override
    public ByteOrder byteOrder() {
      return java.nio.ByteOrder.BIG_ENDIAN;
    }

  }

  final Context ctx = new Context();

  final FiniteDuration interval = Duration.apply(1, TimeUnit.SECONDS);

  final PipelineStage<Context, Message, ByteString, Message, ByteString> stages = //
  PipelineStage.sequence(PipelineStage.sequence( //
      new TickGenerator<Message>(interval), //
      new MessageStage()), //
      new LengthFieldFrame(10000));

  private final ActorRef evts;
  private final ActorRef cmds;

  final PipelineInjector<Message, ByteString> injector = PipelineFactory
      .buildWithSink(ctx, stages, new PipelineSink<ByteString, Message>() {

        @Override
        public void onCommand(ByteString cmd) {
          cmds.tell(cmd, getSelf());
        }

        @Override
        public void onCommandFailure(Throwable thr) throws Throwable {
          throw thr;
        }

        @Override
        public void onEvent(Message evt) {
          evts.tell(evt, getSelf());
        }

        @Override
        public void onEventFailure(Throwable thr) throws Throwable {
          throw thr;
        }
      });

  public Processor(ActorRef cmds, ActorRef evts) throws Exception {
    this.cmds = cmds;
    this.evts = evts;
  }
  
  @Override
  public void preStart() throws Exception {
    injector.managementCommand(TickGenerator.Tick());
  }
  
  @Override
  public void postRestart(Throwable thr) {
    // do not call preStart() again to start the Tick only ONCE
  }

  @Override
  public void onReceive(Object obj) throws Exception {
    if (obj instanceof Message) {
      injector.injectCommand((Message) obj);
    } else if (obj instanceof ByteString) {
      injector.injectEvent((ByteString) obj);
    } else if (obj.equals(TickGenerator.Tick())) {
      injector.managementCommand(obj);
    }
  }

}
