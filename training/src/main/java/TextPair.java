import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author goutamtadi
 *
 *         Holds two Text Objects.
 */
public class TextPair implements WritableComparable {

	private Text textOne;
	private Text textTwo;

	public TextPair() {
	}

	public TextPair(Text textOne, Text textTwo) {
		setTextOne(textOne);
		setTextTwo(textTwo);
	}

	public Text getTextOne() {
		return textOne;
	}

	public void setTextOne(Text textOne) {
		this.textOne = textOne;
	}

	public Text getTextTwo() {
		return textTwo;
	}

	public void setTextTwo(Text textTwo) {
		this.textTwo = textTwo;
	}
	
	public void setTextOne(String textOne){
		this.textOne = new Text(textOne);
	}
	
	public void setTextTwo(String textTwo){
		this.textTwo = new Text(textTwo);
	}

	@Override
	public void readFields(DataInput dataIn) throws IOException {
		textOne.readFields(dataIn);
		textTwo.readFields(dataIn);
	}

	@Override
	public void write(DataOutput dataOut) throws IOException {
		textOne.write(dataOut);
		textTwo.write(dataOut);
	}

	@Override
	public int compareTo(Object o) {
		TextPair tp2 = (TextPair) o;
        int cmp = getTextOne().compareTo(tp2.getTextOne());
        if (cmp != 0) {
              return cmp;
        }
              return getTextTwo().compareTo(tp2.getTextTwo()); // reverse
		
	}
	
	

}
