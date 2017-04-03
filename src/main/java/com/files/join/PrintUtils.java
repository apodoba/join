package com.files.join;

import java.util.List;

public class PrintUtils {
    
    private static final String DIVIDER = "|";
    
    /**
     * Print table content to console
     * @param table
     */
    public static void printTable(Table table){
        if(table.getHeaders() != null && table.getRows() != null){
            printElements(table.getHeaders());
            table.getRows().stream().forEach(element -> printElements(element));
        }
    }
    
    /**
     * Print row from table
     * @param list
     */
    private static void printElements(List<String> list){
        list.stream().map(elem -> elem.concat(DIVIDER)).forEach(System.out::print);
        System.out.println();
    }
}
