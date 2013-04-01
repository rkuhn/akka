/**
 * Copyright (C) 2013 Typesafe Inc. <http://www.typesafe.com>
 */

package docs.io.japi;

import java.nio.ByteOrder;

import scala.util.Either;
import akka.actor.ActorRef;
import akka.io.AbstractSymmetricPipePair;
import akka.io.PipePairFactory;
import akka.io.SymmetricPipePair;
import akka.io.SymmetricPipelineStage;
import akka.util.ByteIterator;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;

//#format
public class MessageStage extends
    SymmetricPipelineStage<HasByteOrder, Message, ByteString> {

  @Override
  public SymmetricPipePair<Message, ByteString> apply(final HasByteOrder context) {

    return PipePairFactory
        .create(context, new AbstractSymmetricPipePair<Message, ByteString>() {

          final ByteOrder byteOrder = context.byteOrder();

          private void putString(ByteStringBuilder builder, String str) {
            final byte[] bytes = ByteString.fromString(str, "UTF-8").toArray();
            builder.putInt(bytes.length, byteOrder);
            builder.putBytes(bytes);
          }

          @Override
          public Iterable<Either<Message, ByteString>> onCommand(Message cmd) {
            final ByteStringBuilder builder = new ByteStringBuilder();

            builder.putInt(cmd.getPersons().length, byteOrder);
            for (Message.Person p : cmd.getPersons()) {
              putString(builder, p.getFirst());
              putString(builder, p.getLast());
            }

            builder.putInt(cmd.getHappinessCurve().length, byteOrder);
            builder.putDoubles(cmd.getHappinessCurve(), byteOrder);

            return singleCommand(builder.result());
          }

          //#decoding-omitted
          //#decoding
          private String getString(ByteIterator iter) {
            final int length = iter.getInt(byteOrder);
            final byte[] bytes = new byte[length];
            iter.getBytes(bytes);
            return ByteString.fromArray(bytes).utf8String();
          }

          @Override
          public Iterable<Either<Message, ByteString>> onEvent(ByteString evt) {
            final ByteIterator iter = evt.iterator();

            final int personLength = iter.getInt(byteOrder);
            final Message.Person[] persons = new Message.Person[personLength];
            for (int i = 0; i < personLength; ++i) {
              persons[i] = new Message.Person(getString(iter), getString(iter));
            }

            final int curveLength = iter.getInt(byteOrder);
            final double[] curve = new double[curveLength];
            iter.getDoubles(curve, byteOrder);

            // verify that this was all; could be left out to allow future
            // extensions
            assert iter.isEmpty();

            return singleEvent(new Message(persons, curve));
          }
          //#decoding
          
          ActorRef target = null;
          
          @Override
          public Iterable<Either<Message, ByteString>> onManagementCommand(Object cmd) {
            if (cmd instanceof PipelineTest.SetTarget) {
              target = ((PipelineTest.SetTarget) cmd).getRef();
            } else if (cmd == TickGenerator.tick && target != null) {
              target.tell(TickGenerator.tick, null);
            }
            return nothing();
          }
          //#decoding-omitted

        });

  }

}
//#format
