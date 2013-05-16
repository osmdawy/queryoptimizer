package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

	private Iterator iterator;
	private Integer[] fields;
	private Schema newSchema;
	private boolean closed;

	/**
	 * Constructs a projection, given the underlying iterator and field numbers.
	 */
	public Projection(Iterator iter, Integer... fields) {
		this.iterator = iter;
		this.fields = fields;
		Schema schema = iter.getSchema();
		Schema new_schema = new Schema(fields.length);
		for (int i = 0; i < fields.length; i++) {
			new_schema.initField(i, schema.fieldType(fields[i]),
					schema.fieldLength(fields[i]), schema.fieldName(fields[i]));
		}
		this.newSchema = new_schema;
		this.setSchema(newSchema);
		this.closed = false;
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
		return iterator.hasNext();
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (iterator.hasNext()) {
			Tuple old_tuple = iterator.getNext();
			Tuple new_tuple = new Tuple(newSchema);
			for (int i = 0; i < fields.length; i++) {
				new_tuple.setField(i, old_tuple.getField(fields[i]));
			}
			return new_tuple;
		} else
			return null;

	}

} // public class Projection extends Iterator
