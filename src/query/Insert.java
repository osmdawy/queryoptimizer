package query;

import index.HashIndex;
import global.AttrOperator;
import global.AttrType;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_Insert;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

	/* attributes */
	String fileName;
	Schema schema;
	IndexDesc[] idesc;
	int index_cnt;
	Object[] values;

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if table doesn't exists or values are invalid
	 */
	public Insert(AST_Insert tree) throws QueryException {
		// initialize variables
		fileName = tree.getFileName();
		/* check if table exists */
		QueryCheck.tableExists(fileName);
		/* get schema and indexes */
		schema = Minibase.SystemCatalog.getSchema(fileName);
		idesc = Minibase.SystemCatalog.getIndexes(fileName);
		index_cnt = idesc.length;

		values = tree.getValues();

	} // public Insert(AST_Insert tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		Tuple temp = new Tuple(schema, values);
		RID rid = new HeapFile(fileName).insertRecord(temp.getData());
		UpdateTableCardinality(fileName);
		if (index_cnt != 0) {
			// insert in hashIndex
			for (IndexDesc id : idesc) {
				HashIndex hash_index = new HashIndex(id.indexName);
				String col_name = id.columnName;
				Object o = values[schema.fieldNumber(col_name)];
				SearchKey search_key = new SearchKey(o);
				hash_index.insertEntry(search_key, rid);
			} // for
		}
		// print the output message
		System.out.println("1 rows inserted .");

	} // public void execute()

	void UpdateTableCardinality(String table_name) {

		FileScan scan = new FileScan(Minibase.SystemCatalog.s_rel,
				Minibase.SystemCatalog.f_rel);
		Predicate p = new Predicate(AttrOperator.EQ, AttrType.COLNAME,
				"relName", AttrType.STRING, table_name);
		Tuple t = null;
		while (scan.hasNext()) {
			t = scan.getNext();
			boolean done = false;
			done = p.evaluate(t);
			if (done)
				break;
		}

		int old_rec_num = (Integer) t.getField("recCount");
		t.setField("recCount", old_rec_num + 1);
		Minibase.SystemCatalog.f_rel.updateRecord(scan.getLastRID(),
				t.getData());
	}

} // class Insert implements Plan
