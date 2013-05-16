package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

	/* Attributes */
	HeapFile heap_file;
	HashScan scan;
	HashIndex index;
	SearchKey key;

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
		this.heap_file = file;
		this.setSchema(schema);
		this.scan = index.openScan(key);
		this.key = key;
	}

	/**
	 * Gives a one-line explanation of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		if (scan != null)
			scan.close();
		scan = index.openScan(key);
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		if (scan != null)
			return true;
		return false;

	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		if (scan != null) {
			scan.close();
		}
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if (scan != null)
			return scan.hasNext();
		return false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (scan != null) {
			RID rid = scan.getNext();
			byte[] t = heap_file.selectRecord(rid);
			Tuple tuple = new Tuple(this.getSchema(), t);
			return tuple;
		}
		return null;
	}

} // public class KeyScan extends Iterator
