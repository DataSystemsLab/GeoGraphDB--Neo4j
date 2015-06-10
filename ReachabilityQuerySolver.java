package def;

public interface ReachabilityQuerySolver {
	void Preprocess();
	boolean ReachabilityQuery(int start_id, Rectangle rect);
}
