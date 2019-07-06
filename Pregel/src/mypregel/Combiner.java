package mypregel;

import java.util.ArrayList;

public abstract class Combiner {
	public abstract void combine(ArrayList<Vertex> vertices);
}
