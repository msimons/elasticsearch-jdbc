/**
 * 
 */
package org.elasticsearch.river.jdbc.strategy.table;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;
import org.elasticsearch.river.jdbc.strategy.simple.SimpleValueListener;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.elasticsearch.river.jdbc.strategy.simple.SimpleValueListener.DOUBLE_NILL_VALUE;
import static org.elasticsearch.river.jdbc.strategy.simple.SimpleValueListener.STRING_NILL_VALUE;

/**
 * @author simon00t
 *
 */
public class TableValueListenerTests extends Assert {
	
    @Test
    public void testFilterColumns() throws Exception {
        List<String> columns = Arrays.asList("source_operation", "_id", "label");
        List<String> row1 = Arrays.asList("index", "1", "label1");
        List<String> row2 = Arrays.asList("index", "2", "label2");
        List<String> row3 = Arrays.asList("index", "3", "label3");
        List<String> row4 = Arrays.asList("index", "4", "label4");
        MockRiverMouth target = new MockRiverMouth();
        new TableValueListener()
                .target(target)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .end();
        assertEquals(target.data().size(), 4, "Number of inserted objects");
        assertEquals(target.data().toString(),"{index/null/null/1 {label=\"label1\"}={\"label\":\"label1\"}, " +
        		"index/null/null/2 {label=\"label2\"}={\"label\":\"label2\"}, " +
        				"index/null/null/3 {label=\"label3\"}={\"label\":\"label3\"}, " +
        						"index/null/null/4 {label=\"label4\"}={\"label\":\"label4\"}}");
    }

    @Test
    public void testUpdateWithoutColumn() throws Exception {
        List<String> columns = Arrays.asList("source_operation", "_id", "person.surname","person.lastname","person.age");
        List<Object> row1 = Arrays.asList(new Object[]{"index", "1", "Johnny","Bravo",3});
        List<Object> row2 = Arrays.asList(new Object[]{"index", "1", "Johnny","Bravo",5});
        List<Object> row3 = Arrays.asList(new Object[]{"index", "2", "Mickey",null,null});
        List<Object> row4 = Arrays.asList(new Object[]{"index", "3", "Road", STRING_NILL_VALUE, DOUBLE_NILL_VALUE});
        MockRiverMouth target = new MockRiverMouth();
        new TableValueListener()
                .target(target)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .end();
        assertEquals(target.data().size(), 3, "Number of inserted objects");
        assertEquals(target.data().toString(),"{index/null/null/1 {person={age=[3,5], lastname=\"Bravo\", surname=\"Johnny\"}}={\"person\":{\"age\":[3,5],\"lastname\":\"Bravo\",\"surname\":\"Johnny\"}}," +
                " index/null/null/2 {person={age=null, lastname=null, surname=\"Mickey\"}}={\"person\":{\"age\":null,\"lastname\":null,\"surname\":\"Mickey\"}}," +
                " index/null/null/3 {person={surname=\"Road\"}}={\"person\":{\"surname\":\"Road\"}}}");
    }
    
    @Test
    public void testMergeDelete() throws Exception {
        List<String> columns = Arrays.asList("source_operation", "_id", "label");
        List<String> row1 = Arrays.asList("create", "1", "label1");
        List<String> row2 = Arrays.asList("delete", "1", "label2");
        MockRiverMouth target = new MockRiverMouth();
        new TableValueListener()
                .target(target)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .end();
        assertEquals(target.data().size(), 1, "Number of inserted objects");
    }
    
    @Test
    public void testDocs() throws Exception {
        List<String> columns = Arrays.asList("source_operation", "_id", "label");
        List<String> row1 = Arrays.asList("update", "1", "label1");
        List<String> row2 = Arrays.asList("update", "2", "label1");
        List<String> row3 = Arrays.asList("update", "1", "label2");
        MockRiverMouth target = new MockRiverMouth();
        new TableValueListener()
                .target(target)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .end();
        assertEquals(target.data().size(), 3, "Number of inserted objects");

    }
    
    
    @Test
    public void testMerge() throws Exception {
        List<String> columns = Arrays.asList("source_operation", "_id", "label");
        List<String> row1 = Arrays.asList("create", "1", "label1");
        List<String> row2 = Arrays.asList("update", "1", "label2");
        List<String> row3 = Arrays.asList("update", "1", "label2");
        List<String> row4 = Arrays.asList("update", "1", "label2");

        MockRiverMouth target = new MockRiverMouth();
        new TableValueListener()
                .target(target)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .end();
        assertEquals(target.data().size(), 2, "Number of inserted objects");
    }
}
