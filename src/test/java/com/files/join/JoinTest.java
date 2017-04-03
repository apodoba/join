package com.files.join;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

public class JoinTest {

    @Test
    public void testInnerJoinFullSearch() throws URISyntaxException {
        Table table1 = new Table(getTablePath("employee.csv"));
        Table table2 = new Table(getTablePath("employee_address.csv"));

        Join join = new Join(table1, table2, "id", "employee_id", SEARCH_TYPE.FULL);
        Table table = join.joinTables();
        
        Assert.assertEquals(table.getHeaders().size(), 9);
        Assert.assertEquals(table.getRows().size(), 3);
        
        PrintUtils.printTable(table);
    }
    
    @Test
    public void testInnerJoinBinarySearch() throws URISyntaxException {
        Table table1 = new Table(getTablePath("employee.csv"));
        Table table2 = new Table(getTablePath("employee_address.csv"));

        Join join = new Join(table1, table2, "id", "employee_id", SEARCH_TYPE.BINARY);
        Table table = join.joinTables();
        
        Assert.assertEquals(table.getHeaders().size(), 9);
        Assert.assertEquals(table.getRows().size(), 3);
        
        PrintUtils.printTable(table);
    }

    private Path getTablePath(String fileName) throws URISyntaxException{
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        return Paths.get(classloader.getResource(fileName).toURI());
    }
}
