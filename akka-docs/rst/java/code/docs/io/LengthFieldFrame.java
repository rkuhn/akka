/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package docs.io;

import java.nio.ByteOrder;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import scala.util.Either;
import akka.io.AbstractSymmetricPipePair;
import akka.io.PipePairFactory;
import akka.io.SymmetricPipePair;
import akka.io.SymmetricPipelineStage;
import akka.util.ByteString;

public class LengthFieldFrame extends
    SymmetricPipelineStage<Object, ByteString, ByteString> {

  final int maxSize;

  public LengthFieldFrame(int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  public SymmetricPipePair<ByteString, ByteString> apply(Object unused) {
    return PipePairFactory
        .<ByteString, ByteString> create(new AbstractSymmetricPipePair<ByteString, ByteString>() {

          final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
          ByteString buffer = null;

          @Override
          public Iterable<Either<ByteString, ByteString>> onCommand(
              ByteString cmd) {
            final int length = cmd.length() + 4;
            if (length > maxSize) {
              return new ArrayList<Either<ByteString, ByteString>>(0);
            }
            final ByteBuffer bb = ByteBuffer.allocate(4);
            bb.order(byteOrder);
            bb.putInt(length);
            bb.flip();
            final ArrayList<Either<ByteString, ByteString>> res = new ArrayList<Either<ByteString, ByteString>>(
                1);
            res.add(makeCommand(ByteString.fromByteBuffer(bb).concat(cmd)));
            return res;
          }

          @Override
          public Iterable<Either<ByteString, ByteString>> onEvent(
              ByteString event) {
            final ArrayList<Either<ByteString, ByteString>> res = new ArrayList<Either<ByteString, ByteString>>();
            ByteString current = buffer == null ? event : buffer.concat(event);
            while (true) {
              if (current.length() == 0) {
                buffer = null;
                return res;
              } else if (current.length() < 4) {
                buffer = current;
                return res;
              } else {
                final int length = buffer.iterator().getInt(byteOrder);
                if (length > maxSize)
                  throw new IllegalArgumentException(
                      "received too large frame of size " + length + " (max = "
                          + maxSize + ")");
                if (current.length() < length) {
                  buffer = current;
                  return res;
                } else {
                  res.add(makeEvent(current.slice(4, length)));
                  current = current.drop(length);
                }
              }
            }
          }

        });
  }

}
