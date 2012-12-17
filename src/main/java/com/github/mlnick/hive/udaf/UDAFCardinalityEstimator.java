/*
 * Copyright (C) 2012 Nick Pentreath.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mlnick.hive.udaf;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.LinearCounting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hive Generic UDAF for approximate counting of unique values in a column (cardinality estimation). This UDAF implements
 * HyperLogLog (see http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) and Linear Counting
 * (http://dblab.kaist.ac.kr/Publication/pdf/ACM90_TODS_v15n2.pdf).
 * <p>
 *     The default is HyperLogLog with bit-size parameter = 16, which should achieve error rates of around 0.5%
 *     for reasonably large cardinalities, without too much memory overhead during processing.
 * </p>
 */

@Description(name = "approx_distinct",
        value = "_FUNC_(x, [t], [b]) - Adds values from x to new Cardinality Estimator, with " +
                "optional type t (hll, lc) and bit parameter b." +
                "\nDefaults to HyperLogLog with b=16." +
                "\nReturns a struct {type: string, cardinality: long, binary: binary}, where:" +
                "\n    type        = {HLL, LC} for HyperLogLog or Linear Counting" +
                "\n    cardinality = the esimated cardinality" +
                "\n    binary      = serialized representation of the data structure",
        extended =  "Example:" +
                "\n> SELECT approx_distinct(values) FROM src; -- defaults to HyperLogLog with b=16" +
                "\n> SELECT approx_distinct(values, 'lc') FROM src; -- Linear Counting with b=1000000")
public class UDAFCardinalityEstimator implements GenericUDAFResolver2 {

    public enum CardinalityEstimator {
        HLL("HyperLogLog"),
        LC("LinearCounting");

        private final String type;

        private CardinalityEstimator(String type) {
            this.type = type;

        }

        @Override
        public String toString() {
            return this.type;
        }
    }

    static final Log LOG = LogFactory.getLog(UDAFCardinalityEstimator.class.getName());
    public static final int HLL_DEFAULT_B = 16;
    public static final int LC_DEFAULT_SIZE = 1000000;
    public static final String ESTIMATOR_TYPE = "type";
    public static final String CARDINALITY = "cardinality";
    public static final String BINARY = "binary";

    // TODO byte to id the estimator type?
    //public static final byte HLL = 0;
    //public static final byte LC = 1;

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        TypeInfo[] parameters = info.getParameters();
        // validate the first parameter, which is the expression to compute over
        // can be a primitive or an existing estimator
        if (!(parameters[0].getCategory() == ObjectInspector.Category.PRIMITIVE ||
                parameters[0].getCategory() == ObjectInspector.Category.STRUCT))  {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type or struct arguments are accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1.");
        }

        // validate the second parameter, which is a String cardinality estimator type.
        if (parameters.length >= 2) {
            if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(1,
                        "Only primitive type arguments are accepted but "
                                + parameters[1].getTypeName() + " was passed as parameter 2.");
            }
            if( ((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()
                    != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                throw new UDFArgumentTypeException(1,
                        "Only a String argument is accepted as parameter 2, but "
                                + parameters[1].getTypeName() + " was passed instead.");
            }
        }
        // validate the third parameter, which is an int
        if (parameters.length == 3) {
            if (parameters[2].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(1,
                        "Only primitive type arguments are accepted but "
                                + parameters[2].getTypeName() + " was passed as parameter 2.");
            }
            if( ((PrimitiveTypeInfo) parameters[2]).getPrimitiveCategory()
                    != PrimitiveObjectInspector.PrimitiveCategory.INT) {
                throw new UDFArgumentTypeException(2,
                        "Only a int argument is accepted as parameter 3, but "
                                + parameters[2].getTypeName() + " was passed instead.");
            }
        }

        if (parameters.length > 4) throw new IllegalArgumentException("Function only takes 1, 2 or 3 parameters.");

        return new CardinalityEstimatorEvaluator();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] typeInfos) throws SemanticException {
        return new CardinalityEstimatorEvaluator();
    }

    /**
     * Class to evaluate values, and add them to an approximate cardinality estimator datastructure.
     * Currently HyperLogLog and Linear Counting are supported.
     */
    public static class CardinalityEstimatorEvaluator extends GenericUDAFEvaluator {
        // defaults to HyperLogLog if no type parameter is present in the function call
        CardinalityEstimator ce = CardinalityEstimator.HLL;

        // inputs
        PrimitiveObjectInspector inputPrimitiveOI;
        StructObjectInspector inputStructOI;
        PrimitiveObjectInspector typeOI;
        PrimitiveObjectInspector paramOI;

        // intermediate results
        StandardListObjectInspector partialOI;

        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            // init input object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                assert (parameters.length >= 1 && parameters.length <= 3);
                // we can take PRIMITIVEs or an existing STRUCT containing the
                // serialised cardinality estimator
                ObjectInspector.Category cat = parameters[0].getCategory();
                switch (cat) {
                    case PRIMITIVE:
                        // if PRIMITIVE, parse out the 2nd and 3rd args, if present
                        inputPrimitiveOI = (PrimitiveObjectInspector) parameters[0];
                        switch (parameters.length) {
                            case 2:
                                typeOI = (PrimitiveObjectInspector) parameters[1];
                                break;
                            case 3:
                                typeOI = (PrimitiveObjectInspector) parameters[1];
                                paramOI = (PrimitiveObjectInspector) parameters[2];
                                break;
                        }
                        break;
                    case STRUCT:
                        // if an existing estimator, we don't take the extra args since
                        // we can only merge compatible data structures
                        inputStructOI = (StructObjectInspector) parameters[0];
                        break;
                    default:
                        // should never happen since we already validated the args in getEvaluator
                        throw new IllegalArgumentException("Only PRIMITIVE types and existing STRUCTs " +
                                "containing cardinality estimators are allowed as input. Passed a " + cat.name());
                }
            }
            else {
                // partial input object inspector for intermediate results
                partialOI = (StandardListObjectInspector) parameters[0];
            }

            // output object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
            }
            else {
                ArrayList<String> fNames = new ArrayList<String>();
                fNames.add(ESTIMATOR_TYPE);
                fNames.add(CARDINALITY);
                fNames.add(BINARY);
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
                return ObjectInspectorFactory.getStandardStructObjectInspector(fNames, foi);
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CardinalityEstimatorBuffer ceb = new CardinalityEstimatorBuffer();
            reset(ceb);
            return ceb;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((CardinalityEstimatorBuffer) aggregationBuffer).cardinalityEstimator = null;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            if (parameters[0] == null) {
                return;
            }
            int param = -1;
            if (parameters.length >= 2) {
                String type = PrimitiveObjectInspectorUtils.getString(parameters[1], typeOI);
                ce = CardinalityEstimator.valueOf(type.toUpperCase());
            }
            if (parameters.length == 3) {
                param = PrimitiveObjectInspectorUtils.getInt(parameters[2], paramOI);
            }
            CardinalityEstimatorBuffer ceb = (CardinalityEstimatorBuffer) aggregationBuffer;

            Object obj = parameters[0];
            if (inputPrimitiveOI != null) {
                // in this case, we process the object directly
                if (ceb.cardinalityEstimator == null) {
                    initEstimator(param, ceb);
                }
                ceb.cardinalityEstimator.offer(obj);
            }
            else if (inputStructOI != null) {
                // in this case we merge estimators
                LazyString type = (LazyString) inputStructOI.getStructFieldData(obj, inputStructOI.getStructFieldRef(ESTIMATOR_TYPE));
                LazyBinary lb = (LazyBinary) inputStructOI.getStructFieldData(obj, inputStructOI.getStructFieldRef(BINARY));
                ICardinality that = buildEstimator(lb.getWritableObject(), type.toString());
                mergeEstimators(that, ceb);
            }
        }

        /**
         * Return partial result of aggregation.
         * @param aggregationBuffer current state of the aggregation
         * @return partial result, in the form of a list of {@link BytesWritable} containing the estimator type, and the
         * serialised estimator
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            CardinalityEstimatorBuffer ceb = (CardinalityEstimatorBuffer) aggregationBuffer;
            try {
                byte[] tb = ce.name().getBytes("UTF-8");
                byte[] ceBytes = ceb.cardinalityEstimator.getBytes();
                List<BytesWritable> b = new ArrayList<BytesWritable>();
                b.add(new BytesWritable(tb));
                b.add(new BytesWritable(ceBytes));
                return b;
            } catch (IOException e) {
                throw new HiveException("Failed to extract byte[] during partial termination. ", e);
            }
        }

        /**
         * Merges a partial estimator
         * @param aggregationBuffer current state of the aggregation
         * @param partial estimator object to be merged (see {@link #terminatePartial(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer)}
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            CardinalityEstimatorBuffer ceb = (CardinalityEstimatorBuffer) aggregationBuffer;

            List<BytesWritable> partialResult = (List<BytesWritable>) partialOI.getList(partial);
            assert (partialResult.size() == 2);
            BytesWritable partialString = partialResult.get(0);
            String type = null;
            try {
                type = new String(partialString.getBytes(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new HiveException(e);
            }
            BytesWritable partialBytes = partialResult.get(1);

            // Parse the serialised partial result and merge
            ICardinality partialEstimator = buildEstimator(partialBytes, type);
            mergeEstimators(partialEstimator, ceb);
        }

        /**
         * Return the final state of the aggregation
         * @param aggregationBuffer current state of the aggregation
         * @return Hive struct data type, {type: string, cardinality: bigint, binary: binary}, containing the type of the
         * estimator, the estimated cardinality, and the serialised estimator
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            CardinalityEstimatorBuffer ceb = (CardinalityEstimatorBuffer) aggregationBuffer;
            if (ceb.cardinalityEstimator == null) {
                return null;
            }
            try {
                ArrayList<Object> result = new ArrayList<Object>();
                result.add(new Text(ce.name()));
                long cardinality = ceb.cardinalityEstimator instanceof HyperLogLog
                        ? ((HyperLogLog) ceb.cardinalityEstimator).cardinality(/* use long-range estimate */ false)
                        : ceb.cardinalityEstimator.cardinality();
                result.add(new LongWritable(cardinality));
                result.add(new BytesWritable(ceb.cardinalityEstimator.getBytes()));
                return result;
            } catch (IOException e) {
                throw new HiveException("Failed to extract serialised Cardinality Estimator instance. ", e);
            }
        }

        // HELPER METHODS //

        /**
         * Initialise a new cardinality estimator
         * @param parameter <i>b</i> parameter for HyperLogLog or Linear Counting
         * @param ceb the {@link AggregationBuffer} to initialise
         */
        private void initEstimator(int parameter, CardinalityEstimatorBuffer ceb) {
            switch (ce) {
                case HLL:
                    ceb.cardinalityEstimator = new HyperLogLog(parameter < 0 ? HLL_DEFAULT_B : parameter);
                    break;
                case LC:
                    ceb.cardinalityEstimator = new LinearCounting.Builder(parameter < 0 ? LC_DEFAULT_SIZE : parameter).build();
                    break;
            }
        }

        /**
         * Builds a cardinality estimator from a serialised binary representation.
         * @param bw byte[] representation (wrapped in a BytesWritable object)
         * @param type one of HLL for HyperLogLog, or LC for Linear Counting
         * @return cardinality estimator instance
         * @throws HiveException
         */
        private ICardinality buildEstimator(BytesWritable bw, String type) throws HiveException {
            type = type.trim();
            ce = CardinalityEstimator.valueOf(type);
            ICardinality estimator;
            switch (ce) {
                case HLL:
                    try {
                        estimator = HyperLogLog.Builder.build(bw.getBytes());
                    } catch (IOException e) {
                        throw new HiveException("Failed to parse byte[] from partial result. ", e);
                    }
                    break;
                case LC:
                    estimator = new LinearCounting(bw.getBytes());
                    break;
                default: throw new IllegalArgumentException("Unknown CardinalityEstimator type [" + ce +
                        "], failed to parse partial result");
            }
            return estimator;
        }

        /**
         * Either merge a partial estimator into the current aggregation buffer, or if the buffer is empty,
         * simply set to the partial estimator
         * @param thatEstimator the cardinality estimator to merge in
         * @param thisEstimatorBuffer the current aggregation buffer instance
         * @throws HiveException
         */
        private static void mergeEstimators(ICardinality thatEstimator, CardinalityEstimatorBuffer thisEstimatorBuffer) throws HiveException {
            try {
                if (thisEstimatorBuffer.cardinalityEstimator == null) {
                    thisEstimatorBuffer.cardinalityEstimator = thatEstimator;
                    LOG.debug("Aggregation buffer is null, using THAT partial instance. Cardinality result = " + thisEstimatorBuffer.cardinalityEstimator.cardinality());
                }
                else {
                    LOG.debug("Merging estimator instances, with THIS partial result = " + thisEstimatorBuffer.cardinalityEstimator.cardinality() +
                            " and THAT partial result = " + thatEstimator.cardinality());
                    thisEstimatorBuffer.cardinalityEstimator = thisEstimatorBuffer.cardinalityEstimator.merge(thatEstimator);
                    LOG.debug("MERGED partial result = " + thisEstimatorBuffer.cardinalityEstimator.cardinality());
                }
            } catch (CardinalityMergeException e) {
                throw new HiveException("Failed to merge Cardinality Estimator instances due to cardinality error. ", e);
            }
        }

        /**
         * Wrapper for {@link ICardinality} instance to which values are added
         */
        static class CardinalityEstimatorBuffer implements AggregationBuffer {
            ICardinality cardinalityEstimator;
        }
    }
}