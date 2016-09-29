package edu.buffalo.cse.pigout.declassify;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
	
public class DeclassifyUDF extends EvalFunc<Tuple>
  {
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
        	return input;
        }catch(Exception e){
           throw new IOException("Caught exception processing input row ", e);
        }
    }
  }
