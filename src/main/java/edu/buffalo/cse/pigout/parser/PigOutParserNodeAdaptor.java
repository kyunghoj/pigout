package edu.buffalo.cse.pigout.parser;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.apache.pig.parser.PigParserNode;

public class PigOutParserNodeAdaptor extends CommonTreeAdaptor {
	private String source;
	private int lineOffset;

    PigOutParserNodeAdaptor(String source, int lineOffset) {
        this.source = source;
        this.lineOffset = lineOffset;
    }

    @Override
    public Object create(Token t) {
        return new PigParserNode(t, source, lineOffset);
    }

}
