package def;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import net.sf.jsi.Rectangle;
import net.sf.jsi.SpatialIndex;
import net.sf.jsi.rtree.RTree;
import rx.Observable;

import com.github.davidmoten.rx.Serialized;
//import com.infomatiq.jsi.Rectangle;
//import com.infomatiq.jsi.SpatialIndex;
//import com.infomatiq.jsi.rtree.RTree;

public class ReachabilityRTree_test {
	
	public static void WriteTest() throws IOException
	{
		SpatialIndex si = new RTree();
		si.init(null);
		final Rectangle[] rects = new Rectangle[100];
		rects[0] = new Rectangle(0, 10, 0, 10);
		rects[1] = new Rectangle(0, 11, 1, 20);
		si.add(rects[0], 0);
		si.add(rects[1], 1);
		File sFilePath = new File("/home/yuhansun/test.ser");
		FileOutputStream fos = null;

        ObjectOutputStream ooS = null;

        try {

            fos = new FileOutputStream(sFilePath);

            ooS = new ObjectOutputStream(fos);

            ooS.writeObject(si);

        } catch (Exception ex) {

            ex.printStackTrace();

        } finally {

            if (fos != null)

                fos.close();

            if (ooS != null) {

                ooS.close();

            }

        }
	}
	
	public static RTree ReadTest()
	{
		RTree si = null;
		FileInputStream fis = null;

        ObjectInputStream ois = null;
        
        try
        {
        	fis = new FileInputStream("/home/yuhansun/test.ser");
        	ois = new ObjectInputStream(fis);
        	si = (RTree)ois.readObject();
        	ois.close();
        }
        catch(Exception e)
        {
        	System.out.println(e.getMessage());
        }
		return si;
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		RTree si = ReadTest();
		Rectangle bound = si.getBounds();
		System.out.println(bound.minX);
		System.out.println(bound.minY);
		System.out.println(bound.maxX);
		System.out.println(bound.maxY);
	}

}
