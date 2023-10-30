package edu.whu.tmdb.query.excecute;

import edu.whu.tmdb.query.utils.TMDBException;
import edu.whu.tmdb.query.utils.SelectResult;

import java.io.IOException;

public interface Select {
    SelectResult select(Object stmt) throws TMDBException, IOException;
}
