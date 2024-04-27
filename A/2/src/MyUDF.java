import java.io.IOException;
// import java.util.ArrayList;
import java.util.Iterator;
// import java.util.List;

// import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
// import org.apache.pig.FuncSpec;
// import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
// import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
// import org.apache.pig.impl.logicalLayer.FrontendException;
// import org.apache.pig.impl.logicalLayer.schema.Schema;

public class MyUDF extends EvalFunc<Long> implements Algebraic {

    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    public Long exec(Tuple input) throws IOException {
        return count(input);
    }

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermed.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            return mTupleFactory.newTuple(count(input));
        }
    }

    static public class Intermed extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
            return mTupleFactory.newTuple(sum(input));
        }
    }

    static public class Final extends EvalFunc<Long> {
        public Long exec(Tuple input) throws IOException {
            return sum(input);
        }
    }

    static protected Long sum(Tuple input) throws ExecException, NumberFormatException {
        DataBag values = (DataBag) input.get(0);
        long sum = 0;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            sum += (Long) t.get(0);
        }
        return sum;
    }

    static protected Long count(Tuple input) throws ExecException {
        DataBag bag = (DataBag) input.get(0);
        String sub_input = (String) input.get(1);
        String pred_input = (String) input.get(2);
        Iterator<Tuple> it = bag.iterator();
        // int flag = 1;
        int count = 0;
        if (it.hasNext()) {
            Tuple t = (Tuple) it.next();
            // if (flag == 1) {
            // System.out.println(pred_input + t);
            // System.out.println(t.get(1) + " " + t.get(1).getClass());
            // flag = 0;
            // }

            if (t != null && t.size() > 0 && t.get(0) != null && t.get(0).equals(sub_input)
                    && t.get(1).equals(pred_input))
                count = count + 1;
        }
        return new Long(count);
    }
}