package mypregel;

import java.util.ArrayList;

public abstract class Aggregator {
	public abstract String aggregate();
	public abstract void report(ArrayList<Vertex> verticeslist);
}
