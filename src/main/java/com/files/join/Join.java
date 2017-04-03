package com.files.join;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Process join tables
 *
 */
public class Join {
    
    private Table leftTable;
    private Table rightTable;
    private String leftJoinColumn;
    private String rightJoinColumn;
    private SEARCH_TYPE searchType;
    
    /**
     * @param leftTable - representer of left table in join
     * @param rightTable - representer of right table in join
     * @param leftJoinColumn - join column from left table
     * @param rightJoinColumn - join column from right table
     * @param searchType - can be FULL or BINARY search
     */
    public Join(Table leftTable, Table rightTable, String leftJoinColumn, String rightJoinColumn, SEARCH_TYPE searchType) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.leftJoinColumn = leftJoinColumn;
        this.rightJoinColumn = rightJoinColumn;
        this.searchType = searchType;
    }
    

    /**
     * Execute join tables
     * @return 
     */
    public Table joinTables(){
        Table joinTable = new Table();
        
        if(leftTable.getHeaders().contains(leftJoinColumn) && rightTable.getHeaders().contains(rightJoinColumn)){
            int leftJoinColumnIndex = leftTable.getHeaders().indexOf(leftJoinColumn);
            
            List<List<String>> joinRows = leftTable.getRows()
                .stream()
                .filter(row -> !row.isEmpty() && row.size() > leftJoinColumnIndex)
                .skip(0)
                .map(row -> joinRows(row))
                .filter(row -> row.size() > leftTable.getHeaders().size())
                .collect(Collectors.toList());
            
            List<String> joinHeaders = joinHeaders();
            
            joinTable.setHeaders(joinHeaders);
            joinTable.setRows(joinRows);
        }
        
        return joinTable;
    }
    
    /**
     * Join headers of tables
     * @return
     */
    private List<String> joinHeaders(){
        List<String> joinHeaders = new ArrayList<String>(leftTable.getHeaders());
        joinHeaders.addAll(rightTable.getHeaders());
        return joinHeaders;
    }
    
    /**
     * Join all rows of tables
     * @return
     */
    private List<String> joinRows(List<String> leftTableRow){
        int leftJoinColumnIndex = leftTable.getHeaders().indexOf(leftJoinColumn);
            
        List<String> rightTableRow = findCorrespondingRow(leftTableRow.get(leftJoinColumnIndex));
        
        List<String> joinRows = new ArrayList<String>(leftTableRow);
        joinRows.addAll(rightTableRow);
        return joinRows;
    }
    
    /**
     * Search row for join from right table by value from left table
     * @param joinColumnValue
     * @return
     */
    private List<String> findCorrespondingRow(String joinColumnValue){
        int rightJoinColumnIndex = rightTable.getHeaders().indexOf(rightJoinColumn);
        
        switch (searchType) {
        case BINARY:
            return binarySearch(joinColumnValue, rightJoinColumnIndex);
        default:
            return fullSearch(joinColumnValue, rightJoinColumnIndex);
        }
        
    }

    /**
     * Search row for join by Full search
     * @param joinColumnValue
     * @param rightJoinColumnIndex
     * @return
     */
    private List<String> fullSearch(String joinColumnValue, int rightJoinColumnIndex){
        try(Stream<List<String>> stream = rightTable.getRows().stream()){
            return stream
                    .filter(row -> row.size() > rightJoinColumnIndex && row.get(rightJoinColumnIndex).equals(joinColumnValue))
                    .findFirst()
                    .get();
        }catch (NoSuchElementException e) {
            return Collections.emptyList();
        }
    }
    
    /**
     * Search row for join by binary search in sorted table
     * @param joinColumnValue
     * @param rightJoinColumnIndex
     * @return
     */
    private List<String> binarySearch(String joinColumnValue, int rightJoinColumnIndex){
        List<List<String>> rightTableRows = rightTable.getRows().stream().filter(row -> row.size() > rightJoinColumnIndex).collect(Collectors.toList());
        
        List<String> joinValue = new ArrayList<String>(rightTable.getHeaders());
        joinValue.set(rightJoinColumnIndex, joinColumnValue);
        
        int index = Collections.binarySearch(rightTableRows, joinValue, (row1, row2) -> row1.get(rightJoinColumnIndex).compareTo(row2.get(rightJoinColumnIndex)));
        return index > -1 ? rightTable.getRows().get(index) : new ArrayList<String>();
    }

    public Table getLeftTable() {
        return leftTable;
    }


    public void setLeftTable(Table leftTable) {
        this.leftTable = leftTable;
    }


    public Table getRightTable() {
        return rightTable;
    }


    public void setRightTable(Table rightTable) {
        this.rightTable = rightTable;
    }


    public String getLeftJoinColumn() {
        return leftJoinColumn;
    }


    public void setLeftJoinColumn(String leftJoinColumn) {
        this.leftJoinColumn = leftJoinColumn;
    }


    public String getRightJoinColumn() {
        return rightJoinColumn;
    }


    public void setRightJoinColumn(String rightJoinColumn) {
        this.rightJoinColumn = rightJoinColumn;
    }
}
