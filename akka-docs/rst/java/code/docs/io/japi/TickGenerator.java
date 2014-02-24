/**
 * Copyright (C) 2013 Typesafe Inc. <http://www.typesafe.com>
 */

package docs.io.japi;

import scala.concurrent.duration.FiniteDuration;
import scala.util.Either;
import akka.actor.ActorSystem;
import akka.io.AbstractPipePair;
import akka.io.PipePair;
import akka.io.PipePairFactory;
import akka.io.PipelineStage;

//#tick-generator
public class TickGenerator<Cmd, Evt> extends
    PipelineStage<HasActorContext, Cmd, Cmd, Evt, Evt> {

  public static final Object tick = new Object() {
    public String toString() {
      return "Tick";
    }
  };

  private final FiniteDuration interval;

  public TickGenerator(FiniteDuration interval) {
    this.interval = interval;
  }

  @Override
  public PipePair<Cmd, Cmd, Evt, Evt> apply(final HasActorContext ctx) {
    return PipePairFactory.create(ctx,
        new AbstractPipePair<Cmd, Cmd, Evt, Evt>() {

          @Override
          public Iterable<Either<Evt, Cmd>> onCommand(Cmd cmd) {
            return singleCommand(cmd);
          }

          @Override
          public Iterable<Either<Evt, Cmd>> onEvent(Evt evt) {
            return singleEvent(evt);
          }

          @Override
          public Iterable<Either<Evt, Cmd>> onManagementCommand(Object cmd) {
            if (cmd == tick) {
              final ActorSystem system = ctx.getContext().system();
              system.scheduler().scheduleOnce(interval,
                  ctx.getContext().self(), tick, system.dispatcher(), null);
            }
            return nothing();
          }

        });
  }
}
//#tick-generator
