/* Generated By:JJTree: Do not edit this line. ASTSelectClause.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package io.github.yeyuhl.database.cli.parser;

public
class ASTSelectClause extends SimpleNode {
  public ASTSelectClause(int id) {
    super(id);
  }

  public ASTSelectClause(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=2883af4600a354501448dee4f005cc99 (do not edit this line) */
