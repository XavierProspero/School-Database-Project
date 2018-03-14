
package db61b;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static db61b.Utils.*;

/** A single table in a database.
 *  @author P. N. Hilfinger
 */
class Table {
    /** A new Table whose columns are given by COLUMNTITLES, which may
     *  not contain duplicate names. */
    Table(String[] columnTitles) {
        if (columnTitles.length == 0) {
            throw error("table must have at least one column");
        }
        _size = 0;
        _rowSize = columnTitles.length;

        for (int i = columnTitles.length - 1; i >= 1; i -= 1) {
            for (int j = i - 1; j >= 0; j -= 1) {
                if (columnTitles[i].equals(columnTitles[j])) {
                    throw error("duplicate column name: %s",
                                columnTitles[i]);
                }
            }
        }
        _columns = new ValueList[_rowSize];
        for (int i = 0; i < _columns.length; i++) {
            ValueList temp = new ValueList();
            _columns[i] = temp;
        }
        _titles = columnTitles.clone();
    }

    /** A new Table whose columns are given by COLUMNTITLES. */
    Table(List<String> columnTitles) {
        this(columnTitles.toArray(new String[columnTitles.size()]));
    }

    /** Return the number of columns in this table. */
    public int columns() {
        return _rowSize;
    }

    /** Return the title of the Kth column.  Requires 0 <= K < columns(). */
    public String getTitle(int k) {
        return _titles[k];
    }

    /** Return the number of the column whose title is TITLE, or -1 if
     *  there isn't one. */
    public int findColumn(String title) {
        for (int i = 0; i < _titles.length; i++) {
            if (_titles[i].equals(title)) {
                return i;
            }
        }
        return -1;
    }

    /** Return the number of rows in this table. */
    public int size() {
        return _size;
    }

    /** Return the value of column number COL (0 <= COL < columns())
     *  of record number ROW (0 <= ROW < size()). */
    public String get(int row, int col) {
        try {
            return _columns[col].get(row);
        } catch (IndexOutOfBoundsException excp) {
            throw error("invalid row or column");
        }
    }

    /** Add a new row whose column values are VALUES to me if no equal
     *  row already exists.  Return true if anything was added,
     *  false otherwise. */
    public boolean add(String[] values) {
        if (exists(values)) {
            return false;
        }
        for (int j = 0; j < _rowSize; j++) {
            _columns[j].add(values[j]);
        }
        _index.add(findIndex(values, 0), _size);
        _size++;
        return true;
    }

    /** Checks the rows in this table to see if the String array exists
     * in them. Returns true if it exists already.
     *
     * @param values Checking if this values array already exists.
     * @return matched If this exists or not.
     */
    public boolean exists(String[] values) {
        boolean exists;
        for (int i = 0; i < _size; i++) {
            exists = true;
            for (int j = 0; j < _rowSize; j++) {
                if (!_columns[j].get(_index.get(i)).equals(values[j])) {
                    exists = false;
                    break;
                }
            }
            if (exists) {
                return true;
            }
        }
        return false;
    }

    /** Finds the index of where this list should belong in lexigraphic
     * order. Returns the index.
     *
     * @param values an array that represents a row.
     * @param j For the recursion of this method.
     * @return int which signifies where it belong lexigraphically.
     */
    public int findIndex(String[] values, int j) {
        int n = 0;
        int i = 0;
        boolean pushed = false;
        while (n < _rowSize && i < size()) {
            String comp = _columns[n].get(_index.get(i));
            if (pushed && comp.compareTo(values[n]) < 0) {
                return i + 1;
            } else if (comp.compareTo(values[n]) > 0) {
                return i;
            } else if (comp.compareTo(values[n]) == 0) {
                n++;
                pushed = true;
            } else {
                i++;
            }
        }
        return _size;
    }

    /** Add a new row whose column values are extracted by COLUMNS from
     *  the rows indexed by ROWS, if no equal row already exists.
     *  Return true if anything was added, false otherwise. See
     *  Column.getFrom(Integer...) for a description of how Columns
     *  extract values. */
    public boolean add(List<Column> columns, Integer... rows) {
        String[] ret = new String[rows.length];
        for (int i = 0; i < _rowSize; i++) {
            ret[i] = columns.get(i).getFrom(rows);
        }
        return add(ret);
    }


    /** Read the contents of the file NAME.db, and return as a Table.
     *  Format errors in the .db file cause a DBException. */
    static Table readTable(String name) {
        BufferedReader input;
        Table table;
        input = null;
        table = null;
        try {
            input = new BufferedReader(new FileReader(name + ".db"));
            String header = input.readLine();
            if (header == null) {
                throw error("missing header in DB file");
            }
            String[] columnNames = header.split(",");
            table = new Table(columnNames);
            String line;
            String[] parsedLine;
            while (true) {
                line = input.readLine();
                if (line == null) {
                    break;
                }
                parsedLine = line.split(",");
                table.add(parsedLine);
            }
        } catch (FileNotFoundException e) {
            throw error("could not find %s.db", name);
        } catch (IOException e) {
            throw error("problem reading from %s.db", name);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    /* Ignore IOException */
                }
            }
        }
        return table;
    }

    /** Write the contents of TABLE into the file NAME.db. Any I/O errors
     *  cause a DBException. */
    void writeTable(String name) {
        PrintStream output;
        output = null;
        try {
            String sep;
            sep = "";
            output = new PrintStream(name + ".db");
            String out = "";
            for (int i = 0; i < columns(); i++) {
                out = out.concat(this.getTitle(i));
                out = out.concat(",");
            }
            output.println(out);
            for (int row = 0; row < _size; row++) {
                out = "";
                for (ValueList l : _columns) {
                    out = out.concat(l.get(row));
                    out = out.concat(",");
                }
                output.println(out);
            }
        } catch (IOException e) {
            throw error("trouble writing to %s.db", name);
        } finally {
            if (output != null) {
                output.close();
            }
        }
    }

    /** Print my contents on the standard output, separated by spaces
     *  and indented by two spaces. */
    void print() {
        for (int i = 0; i < _size; i++) {
            System.out.print("  ");
            for (ArrayList<String> l : _columns) {
                System.out.print(l.get(_index.get(i)));
                System.out.print(" ");
            }
            System.out.println();
        }
    }


    /** Return a new Table whose columns are COLUMNNAMES, selected from
     *  rows of this table that satisfy CONDITIONS. */
    Table select(List<String> columnNames, List<Condition> conditions) {
        Table result = new Table(columnNames);
        ArrayList<Integer> columnIndeces = new ArrayList<>();
        for (String s : columnNames) {
            if (this.findColumn(s) != -1) {
                columnIndeces.add(findColumn(s));
            }
        }
        String[] temp = new String[columnNames.size()];
        boolean passed;
        for (int t1 = 0; t1 < size(); t1++) {
            passed = checkTable(t1, conditions);
            if (passed) {
                for (int x = 0; x < columnIndeces.size(); x++) {
                    temp[x] = _columns[columnIndeces.get(x)].get(t1);
                }
                result.add(temp);
            }
        }
        return result;
    }

    /** Helper method for determining if a row should be added in select.
     *
     * @param t1 the row in question
     * @param conditions the list of conditions that it has to pass
     * @return whether or not the row should be added
     */
    boolean checkTable(int t1, List<Condition> conditions) {
        if (conditions == null) {
            return true;
        }
        if (_table2 == null) {
            return Condition.test(conditions, t1, t1);
        }
        for (int t2 = 0; t2 < _table2.size(); t2++) {
            if (Condition.test(conditions, t1, t2)) {
                return true;
            }
        }
        return false;
    }


    /** Return a new Table whose columns are COLUMNNAMES, selected
     *  from pairs of rows from this table and from TABLE2 that match
     *  on all columns with identical names and satisfy CONDITIONS. */
    Table select(Table table2, List<String> columnNames,
                 List<Condition> conditions) {
        Table ret = new Table(columnNames);
        String[] temp = new String[columnNames.size()];
        ArrayList<Column> t1Cols = new ArrayList<>();
        ArrayList<Column> t2Cols = new ArrayList<>();
        for (int i = 0; i < _rowSize; i++) {
            String title = getTitle(i);
            int t2 = table2.findColumn(title);
            if (t2 != -1) {
                t1Cols.add(new Column(title, this));
                t2Cols.add(new Column(title, table2));
            }
        }
        for (int row1 = 0; row1 < size(); row1++) {
            for (int row2 = 0; row2 < table2.size(); row2++) {
                if (conditions == null || Condition.test(conditions,
                        row1, row2)) {
                    if (Table.equijoin(t1Cols, t2Cols,
                            row1, row2)) {
                        for (int col = 0; col < columnNames.size(); col++) {
                            String column = columnNames.get(col);
                            if (table2.findColumn(column) != -1) {
                                temp[col] = table2.get(row2, table2.findColumn(
                                        column));
                            } else {
                                temp[col] = get(row1, findColumn(column));
                            }
                        }
                        ret.add(temp);
                        break;
                    }
                }
            }
        }
        return ret;
    }

    /** Return <0, 0, or >0 depending on whether the row formed from
     *  the elements _columns[0].get(K0), _columns[1].get(K0), ...
     *  is less than, equal to, or greater than that formed from elememts
     *  _columns[0].get(K1), _columns[1].get(K1), ....  This method ignores
     *  the _index. */
    private int compareRows(int k0, int k1) {
        for (int i = 0; i < _columns.length; i += 1) {
            int c = _columns[i].get(k0).compareTo(_columns[i].get(k1));
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    /** Return true if the columns COMMON1 from ROW1 and COMMON2 from
     *  ROW2 all have identical values.  Assumes that COMMON1 and
     *  COMMON2 have the same number of elements and the same names,
     *  that the columns in COMMON1 apply to this table, those in
     *  COMMON2 to another, and that ROW1 and ROW2 are indices, respectively,
     *  into those tables. */
    private static boolean equijoin(List<Column> common1, List<Column> common2,
                                    int row1, int row2) {
        for (int i = 0; i < common1.size(); i++) {
            String cr1 = common1.get(i).getFrom(row1);
            String cr2 = common2.get(i).getFrom(row2);
            if (!cr1.equals(cr2)) {
                return false;
            }
        }
        return true;
    }

    /** A class that is essentially ArrayList<String>.  For technical reasons,
     *  we need to encapsulate ArrayList<String> like this because the
     *  underlying design of Java does not properly distinguish between
     *  different kinds of ArrayList at runtime (e.g., if you have a
     *  variable of type Object that was created from an ArrayList, there is
     *  no way to determine in general whether it is an ArrayList<String>,
     *  ArrayList<Integer>, or ArrayList<Object>).  This leads to annoying
     *  compiler warnings.  The trick of defining a new type avoids this
     *  issue. */
    private static class ValueList extends ArrayList<String> {
        /** calling super constructer. */
        ValueList() {
            super(20);
        }
    }

    /** My column titles. */
    private final String[] _titles;

    /** My columns. Row i consists of _columns[k].get(i) for all k. */
    private final ValueList[] _columns;

    /** Rows in the database are supposed to be sorted. To do so, we
     *  have a list whose kth element is the index in each column
     *  of the value of that column for the kth row in lexicographic order.
     *  That is, the first row (smallest in lexicographic order)
     *  is at position _index.get(0) in _columns[0], _columns[1], ...
     *  and the kth row in lexicographic order in at position _index.get(k).
     *  When a new row is inserted, insert its index at the appropriate
     *  place in this list.
     *  (Alternatively, we could simply keep each column in the proper order
     *  so that we would not need _index.  But that would mean that inserting
     *  a new row would require rearranging _rowSize lists (each list in
     *  _columns) rather than just one. */
    private final ArrayList<Integer> _index = new ArrayList<>();

    /** My number of rows (redundant, but convenient). */
    private int _size;
    /** My number of columns (redundant, but convenient). */
    private final int _rowSize;
    /** Second table for select that is compared to. */
    private Table _table2;
}
