/**
 * Copyright (C) 2013 Typesafe Inc. <http://www.typesafe.com>
 */

package docs.io.japi;

import java.nio.ByteOrder;
import java.util.ArrayList;

import akka.io.PipePairFactory;
import akka.io.AbstractSymmetricPipePair;
import akka.io.SymmetricPipePair;
import akka.io.SymmetricPipelineStage;
import akka.util.ByteIterator;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import scala.util.Either;

public class MessageStage extends
    SymmetricPipelineStage<HasByteOrder, Message, ByteString> {

  @Override
  public SymmetricPipePair<Message, ByteString> apply(final HasByteOrder ctx) {

    return PipePairFactory
        .create(new AbstractSymmetricPipePair<Message, ByteString>() {

          final ByteOrder byteOrder = ctx.byteOrder();

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

            final ArrayList<Either<Message, ByteString>> res = new ArrayList<Either<Message, ByteString>>(
                1);
            res.add(makeCommand(builder.result()));
            return res;
          }

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

            final ArrayList<Either<Message, ByteString>> res = new ArrayList<Either<Message, ByteString>>(
                1);
            res.add(makeEvent(new Message(persons, curve)));
            return res;
          }

        });

  }

}
