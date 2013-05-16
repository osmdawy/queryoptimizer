package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {
        
        private Iterator liter;
        private Iterator riter;
        private Predicate[] predicates; 
        private Tuple ltuple;
        private Tuple _saved;
        private boolean _is_open;

  /**
   * Constructs a join, given the left and right iterators and join predicates
   * (relative to the combined schema).
   */
  public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
          liter = left;
          riter = right;                // you cud call here, liter.restart() and riter.restart() to make sure in this ctor, u always do fresh start
          predicates = preds;
          this.setSchema(Schema.join(left.getSchema(), right.getSchema()));
          ltuple = liter.getNext();
          _saved = null;
          _is_open = true;                                              // vkc: i think this var is needed ... and it is changed on basis of internal iterators
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
         // TODO: What to do??
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    liter.restart();
    riter.restart();
    _is_open = true;
    _saved = null;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //return liter.isOpen() & riter.isOpen();
          return _is_open;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    liter.close();
    riter.close();
    _is_open = false;
    _saved = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
          // Assuming the liter is the outer relation.
          /*
           * If you have a valid _saved, return that,
           * else dig further with help of iterators and see if there is any existing tuple that satisfy the predicate which u can 
           * deliver to caller and save that in _saved
           */
          if (_saved != null){
                  return true;
          }

          Tuple rtuple;
          Tuple newtuple = new Tuple(getSchema());
          boolean eval = true;
boolean check =false;
          while (liter.hasNext()) {
                  if (!riter.hasNext()) {
                          ltuple = liter.getNext();
                          riter.restart();
                  }
                  do {
                          eval = true;
                          rtuple = riter.getNext();
                          newtuple = Tuple.join(ltuple, rtuple, getSchema());
                          for (Predicate p : predicates) {
                                  if (!p.evaluate(newtuple)) {
                                          eval = false;
                                          break;
                                  }
                          }
                          if (eval) {
                                  _saved = newtuple;
                                  return true;
                          }
                  } while (riter.hasNext());
                  check = true;
          }

         if(!check){
        	 if(ltuple!=null && riter.hasNext()){
        	
             do {
                     eval = true;
                     
                     
                     rtuple = riter.getNext();
                     
                     newtuple = Tuple.join(ltuple, rtuple, getSchema());
                     for (Predicate p : predicates) {
                             if (!p.evaluate(newtuple)) {
                                     eval = false;
                                     break;
                             }
                     }
                     if (eval) {
                             _saved = newtuple;
                             return true;
                     }
             } while (riter.hasNext()); 
        	 }
         }
          return false;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
        public Tuple getNext() {
                if (_saved != null){
                        Tuple t = _saved;
//                        System.out.println("Saved");
//                        _saved.print();
//                        System.out.println("------------------------------");
                        _saved = null;
                        return t;
                }
                boolean check =false;
                Tuple rtuple;
                Tuple newtuple = new Tuple(getSchema());
                boolean eval = true;

                while (liter.hasNext()) {
                        if (!riter.hasNext()) {
                                ltuple = liter.getNext();
                                riter.restart();
                        }
                        ltuple.print();
                      
                        do {
                                eval = true;
                                rtuple = riter.getNext();
                                rtuple.print();
                                newtuple = Tuple.join(ltuple, rtuple, getSchema());
                                newtuple.print();
                                for (Predicate p : predicates) {
                                        if (!p.evaluate(newtuple)) {
                                                eval = false;
                                                break;
                                        }
                                }
                                if (eval) {
                                        break;
                                }
                        } while (riter.hasNext());
                        if (eval) {
                                return newtuple;
                        }
                        check=true;
                }
                if(!check){
               	 if(ltuple!=null && riter.hasNext()){
               		 do {
                         eval = true;
                         rtuple = riter.getNext();
                         rtuple.print();
                         newtuple = Tuple.join(ltuple, rtuple, getSchema());
                         newtuple.print();
                         for (Predicate p : predicates) {
                                 if (!p.evaluate(newtuple)) {
                                         eval = false;
                                         break;
                                 }
                         }
                         if (eval) {
                                 break;
                         }
                 } while (riter.hasNext());
                 if (eval) {
                         return newtuple;
                 } 
               	 }
               	 }
                
                throw new IllegalStateException ("No new tuple found");
        }

} // public class SimpleJoin extends Iterator