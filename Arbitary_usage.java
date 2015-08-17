package def;

import java.util.Random;

public class Arbitary_usage {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Random r = new Random();
		for(long i = 0;i<10000000;i++)
		{
			for(long j = 0;j<10000000;j++)
				for(long k = 0;k<100000000;k++)
					System.out.println(r.nextDouble());
		}
	}

}
