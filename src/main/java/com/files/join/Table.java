package com.files.join;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represent table view
 *
 */
public class Table {
    
    private static final String DIVIDER = ",";
    
    private List<String> headers;
    
    private List<List<String>> rows;
    
    private Path path;
    
    public Table() {
    }
    
    public Table(List<String> headers, List<List<String>> rows) {
        this.headers = headers;
        this.rows = rows;
    }
    
    public Table(Path path) {
        this.path = path;
        this.headers = constructHeaders();
        this.rows = constructRows();
    }
    
    /**
     * Get table headers from file
     * @return
     */
    private List<String> constructHeaders(){
        try(Stream<String> stream = Files.lines(path)) {
            return stream
                    .filter(header -> !header.isEmpty())
                    .findFirst()
                    .map(header -> Arrays.asList(header.split(DIVIDER)))
                    .get();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    
    /**
     * Get table rows from file
     * @return
     */
    private List<List<String>> constructRows(){
        try(Stream<String> stream = Files.lines(path)) {
            return stream
                    .skip(1)
                    .parallel()
                    .filter(row -> !row.isEmpty())
                    .map(row -> Arrays.asList(row.split(DIVIDER)))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<String> getHeaders() {
        return headers;
    }

    public void setHeaders(List<String> headers) {
        this.headers = headers;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public void setRows(List<List<String>> rows) {
        this.rows = rows;
    }
    
}
