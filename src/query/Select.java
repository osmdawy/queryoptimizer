package query;

import index.HashIndex;

import java.util.ArrayList;
import java.util.HashMap;

import global.AttrOperator;
import global.AttrType;
import global.Minibase;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.KeyScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

	protected String[] table_names;
	protected String[] column_names;
	protected Predicate[][] predicates;
	protected Projection p;
	protected Selection s;
	protected Integer[] cols;
	private HashMap<String, IndexDesc> columnIndexes = new HashMap<String, IndexDesc>();
	protected ArrayList<Iterator> table_itrs_list = new ArrayList<Iterator>();
	protected Iterator final_itr;
	private relop.Iterator tmp;

	// get iterator for a table given the name of the table
	public static Iterator get_table_itr(String table_name) {
		HeapFile file = new HeapFile(table_name);
		Schema schema = Minibase.SystemCatalog.getSchema(table_name);
		return new FileScan(schema, file);
	}

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if validation fails
	 */
	public Select(AST_Select tree) throws QueryException {
		// data from the parse tree
		table_names = tree.getTables();
		column_names = tree.getColumns();
		predicates = tree.getPredicates();

		// check the tables are valid
		for (String tabname : table_names) {
			QueryCheck.tableExists(tabname);
		}

		// Initialize table_itrs_list
		for (int i = 0; i < table_names.length; i++) {
			table_itrs_list.add(get_table_itr(table_names[i]));
			// Initialize columnIndexes
			IndexDesc[] desc = Minibase.SystemCatalog
					.getIndexes(table_names[i]);
			for (IndexDesc id : desc) {
				columnIndexes.put(id.columnName, id);
			}

		}

		// the original predicates given in the query
		ArrayList<ArrayList<Predicate>> o_predicates = new ArrayList<ArrayList<Predicate>>();
		for (Predicate[] arrp : predicates) {
			ArrayList<Predicate> list = new ArrayList<Predicate>();
			for (Predicate p : arrp) {
				list.add(p);
			}
			o_predicates.add(list);
		}

		// keep the applied predicates in order not to apply it again
		HashMap<String, Integer> removedPreds = new HashMap<String, Integer>();

		int table_no = -1;
		for (Iterator i : table_itrs_list) {
			table_no++;
			for (int j = 0; j < predicates.length; j++) {
				Predicate[] arrp = predicates[j];
				ArrayList<Predicate> pList = o_predicates.get(j);
				for (Predicate p : arrp) {
					Schema s = Minibase.SystemCatalog
							.getSchema(table_names[table_no]);

					// This predicate is not in remPred. This means it has
					// not been applied yet.
					switch (p.getRtype()) {
					case AttrType.FLOAT:
					case AttrType.INTEGER:
					case AttrType.STRING:
						// Apply this predicate only if it has not been applied
						// previously
						if (p.validate(s)) {
							if (!removedPreds.containsKey(p.toString())) {

								// If the column in the predicate is indexed,
								// use key scan
								if (columnIndexes.containsKey(p.getLeft())) {
									IndexDesc id = columnIndexes.get(p
											.getLeft());
									HashIndex hi = new HashIndex(id.indexName);
									//table_itrs_list.get(table_no).close();
									KeyScan ks = new KeyScan(s, hi,
											new SearchKey(p.getRight()),
											new HeapFile(id.indexName));
									table_itrs_list.set(table_no, ks);
									System.out.println("Done with "
											+ p.toString());
								} else {
									//table_itrs_list.get(table_no).close();
									table_itrs_list.set(table_no,
											new Selection(i, p));
									System.out.println("Done with "
											+ p.toString());
								}
								// Put this to remPred so that it will not be
								// applied agn
								removedPreds.put(p.toString(), 0);
							}
						}
						if (pList != null)
							pList.remove(p);
						break;
					case AttrType.COLNAME:
					case AttrType.FIELDNO:

						break;
					default:
						break;
					}
				}

				o_predicates.set(j, pList);

			}
		}

		final_itr = table_itrs_list.get(0); // get_table_itr(table_names[0]);
		Schema final_schema = Minibase.SystemCatalog.getSchema(table_names[0]);

		if (table_names.length >= 2) {
			tmp = null;

			for (int i = 1; i < table_names.length; i++) {
				tmp = table_itrs_list.get(i); // get_table_itr (table_names[i]);
				// choose which relation to be outer and which one to be inner
				if (getTableCardinality(table_names[i - 1]) < getTableCardinality(table_names[i])) {

					final_itr = new SimpleJoin(final_itr, tmp);
					final_schema = Schema.join(final_schema,
							Minibase.SystemCatalog.getSchema(table_names[i]));
				} else {
					final_itr = new SimpleJoin(tmp, final_itr);
					final_schema = Schema.join(
							Minibase.SystemCatalog.getSchema(table_names[i]),
							final_schema);
				}
			}
		}

		// Selection on the fly
		if (o_predicates.size() > 0) {
			ArrayList<Predicate> predicate0 = o_predicates.remove(0);
			Predicate[] arr = new Predicate[predicate0.size()];
			predicate0.toArray(arr);
			s = new Selection(final_itr, arr);

			for (ArrayList<Predicate> ap : o_predicates) {
				if (ap.size() != 0) {
					arr = new Predicate[ap.size()];
					ap.toArray(arr);

					s = new Selection(s, arr);
				}
			}
		} else {
			// make a new always true predicate.
			Predicate alwaysTrue = new Predicate(AttrOperator.EQ,
					AttrType.FIELDNO, 1, AttrType.FIELDNO, 1);
			s = new Selection(final_itr, alwaysTrue);
		}

		// Projection
		int column_cnt = column_names.length;
		if (column_cnt > 0) {
			cols = new Integer[column_cnt];
			int i = 0;
			for (String col : column_names) {
				cols[i++] = final_schema.fieldNumber(col);
			}
		} else {
			cols = new Integer[final_schema.getCount()];
			for (int i = 0; i < final_schema.getCount(); i++) {
				cols[i] = i;
			}
		}

		p = new Projection(s, cols);

	} // public Select(AST_Select tree) throws QueryException

	// private method to get Table Cardinality
	private int getTableCardinality(String table_name) {

		Predicate p = new Predicate(AttrOperator.EQ, AttrType.COLNAME,
				"relName", AttrType.STRING, table_name);
		FileScan scan = new FileScan(Minibase.SystemCatalog.s_rel,
				Minibase.SystemCatalog.f_rel);
		Selection s = new Selection(scan, p);
		Tuple t = s.getNext();
		int rec_num = (Integer) t.getField("recCount");

		return rec_num;

	}

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {

		System.out.println("shimaaa   "+s.hasNext());
		p.execute();
		//s.execute();
		p.close();
		s.close();
		final_itr.close();
		for (Iterator i : table_itrs_list) {
			i.close();
		}
		if (tmp != null)
			tmp.close();
	} // public void execute()

} // class Select implements Plan
