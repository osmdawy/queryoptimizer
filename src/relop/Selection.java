package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by AND operators.
 */

public class Selection extends Iterator {

	private Iterator iterator;
	private Predicate[] predicate;
	private Tuple curr_tuple;
	private boolean closed;

	/**
	 * Constructs a selection, given the underlying iterator and predicates.
	 */
	public Selection(Iterator iter, Predicate... preds) {
		this.iterator = iter;
		this.predicate = preds;
		this.closed = false;
		this.setSchema(iter.getSchema());
		curr_tuple = null;
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		iterator.restart();
		closed = false;
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return (!closed);
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		iterator.close();
		closed = true;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if (curr_tuple != null) {
			return true;
		}

		while (iterator.hasNext()) {
			if (predicate.length == 0) {
				return true;
			}
			curr_tuple = iterator.getNext();
			
			boolean done = false;
			for (int i = 0; i < predicate.length; i++) {
				done = predicate[i].evaluate(curr_tuple);
				if (done)
					return true;
			}
		}
		curr_tuple = null;
		return false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (curr_tuple != null) {
			Tuple t = curr_tuple;
			curr_tuple = null;
			return t;
		}
		while (iterator.hasNext()) {
			Tuple t = iterator.getNext();
			if (predicate.length == 0)
				return t;
			boolean done = false;
			for (int i = 0; i < predicate.length; i++) {
				done = predicate[i].evaluate(t);
				if (done)
					return t;
			}
		}
		return null;
	}

} // public class Selection extends Iterator
