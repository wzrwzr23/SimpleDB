package simpledb.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    private int tupleNum;
    private PageId pageId;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *                the pageid of the page on which the tuple resides
     * @param tupleno
     *                the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.tupleNum = tupleno;
        this.pageId = pid;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return tupleNum;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (o instanceof RecordId) {
            RecordId comparedRecord = (RecordId) o;
            return comparedRecord.pageId.equals(this.pageId) &&
                    comparedRecord.tupleNum == this.tupleNum;
        }

        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.pageId, this.tupleNum);

    }

}
