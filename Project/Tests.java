package db61b;
import static org.junit.Assert.*;
import org.junit.Test;

public class Tests {

    /** Testing Tables class.
     */
    @Test
    public void testTables() {
        String[] colVals = {"NUMBERS", "LOWER LETTERS",
            "UPPER LETTERS"
        };
        String[] add = {"1", "a", "A"};
        String[] add2 = {"2", "b", "B"};
        Table test = new Table(colVals);
        test.print();
        test.add(add2);
        assertEquals(test.add(add), true);
        test.print();
        assertEquals(1, 1);
        Table testLoad = Table.readTable("blank");
    }

    /** Testing io capabilities of the function in Tables
     *
     */
    @Test
    public void testIO() {
        String[] compStr = { "First", "Second", "Third" };
        Table comp = new Table(compStr);
        Table compRead = Table.readTable("blank");
    }

    /** Testing database.
     */
    @Test
    public void testDatabase() {
        String[] colVals = {"NUMBERS", "LOWER LETTERS",
            "UPPER LETTERS"
        };
        String[] add = {"1", "a", "A"};
        String title = "Test table";
        Table table = new Table(colVals);
        table.add(add);
        Database d = new Database();
        d.put(title, table);
        d.get(title).print();
    }

}
