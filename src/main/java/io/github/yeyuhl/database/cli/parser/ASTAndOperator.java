/* Generated By:JJTree: Do not edit this line. ASTAndOperator.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package io.github.yeyuhl.database.cli.parser;

public
class ASTAndOperator extends SimpleNode {
  public ASTAndOperator(int id) {
    super(id);
  }

  public ASTAndOperator(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=bcb7f4ac4c15ffc10d073c543052573f (do not edit this line) */