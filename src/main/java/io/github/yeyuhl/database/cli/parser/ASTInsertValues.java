/* Generated By:JJTree: Do not edit this line. ASTInsertValues.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package io.github.yeyuhl.database.cli.parser;

public
class ASTInsertValues extends SimpleNode {
  public ASTInsertValues(int id) {
    super(id);
  }

  public ASTInsertValues(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=eb88f734df3c1399f7b8325ef8bf1d84 (do not edit this line) */
