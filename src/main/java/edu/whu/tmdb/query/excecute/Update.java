package edu.whu.tmdb.query.excecute;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.util.ArrayList;

import edu.whu.tmdb.query.utils.TMDBException;

public interface Update {
    ArrayList<Integer> update(Statement stmt) throws JSQLParserException, TMDBException, IOException;
}
