import static org.junit.Assert.assertEquals;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestTextPair {

	@Test
	public void testTextPair(){
		TextPair tp = new TextPair();
		tp.setTextOne("text1");
		tp.setTextTwo("text2");
		assertEquals("text1",tp.getTextOne().toString());
	}
	
	@Test
	public void testTextPairs(){
		TextPair tp1 = new TextPair();
		tp1.setTextOne("text1");
		tp1.setTextTwo("text2");
		Text t1 = new Text("text1");
		Text t2 = new Text("text2");
		TextPair tp2 = new TextPair(t1,t2);
		assertEquals(0,tp1.compareTo(tp2));
	}
}
