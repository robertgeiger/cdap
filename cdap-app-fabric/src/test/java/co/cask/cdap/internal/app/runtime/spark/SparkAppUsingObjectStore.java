/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * A dummy app with spark program which counts the characters in a string
 */
public class SparkAppUsingObjectStore extends AbstractApplication {

  public static final String APP_NAME = "SparkAppUsingObjectStore";
  public static final String SPARK_NAME = "SparkCharCountProgram";

  @Override
  public void configure() {
    try {
      setName(APP_NAME);
      setDescription("Application with Spark program using objectstore as dataset");
      createDataset("count", KeyValueTable.class);
      createDataset("totals", Table.class);
      ObjectStores.createObjectStore(getConfigurer(), "keys", String.class);
      addSpark(new CharCountSpecification());
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  public static final class CharCountSpecification extends AbstractSpark {
    @Override
    public void configure() {
      setName(SPARK_NAME);
      setDescription("Use Objectstore dataset as input job");
      setMainClass(CharCountProgram.class);
    }

    @Override
    public void beforeSubmit(SparkContext context) throws Exception {

      context.setSparkConf(new SparkConf().set("spark.io.compression.codec",
                                               "org.apache.spark.io.LZFCompressionCodec"));

      Table totals = context.getDataset("totals");
      totals.get(new Get("total").add("total")).getLong("total");
      totals.put(new Put("total").add("total", 0L));
    }
  }

  public static class CharCountProgram implements JavaSparkProgram {
    @Override
    public void run(SparkContext context) {
      JavaSparkContext sc = context.getOriginalSparkContext();
      // Verify the codec is being set
      Preconditions.checkArgument(
        "org.apache.spark.io.LZFCompressionCodec".equals(sc.getConf().get("spark.io.compression.codec")));

                                  // read the dataset
      JavaPairRDD<byte[], String> inputData = context.readFromDataset("keys", byte[].class, String.class);

      // create a new RDD with the same key but with a new value which is the length of the string
      JavaPairRDD<byte[], byte[]> stringLengths = inputData.mapToPair(new PairFunction<Tuple2<byte[], String>,
        byte[], byte[]>() {
        @Override
        public Tuple2<byte[], byte[]> call(Tuple2<byte[], String> stringTuple2) throws Exception {
          return new Tuple2<>(stringTuple2._1(), Bytes.toBytes(stringTuple2._2().length()));
        }
      });

      // write a total count to a table (that emits a metric we can validate in the test case)
      long count = stringLengths.count();
      Table totals = context.getDataset("totals");
      totals.increment(new Increment("total").add("total", count));

      // write the character count to dataset
      context.writeToDataset(stringLengths, "count", byte[].class, byte[].class);
    }
  }
}
