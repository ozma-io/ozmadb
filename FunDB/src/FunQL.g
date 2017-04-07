// Based on PlSQL grammar, under Apache 2.0 license:
// https://github.com/antlr/grammars-v4/blob/master/plsql/plsql.g4

grammar FunQL;

// Tokens

fragment
SIMPLE_LETTER
  : 'a'..'z'
  | 'A'..'Z'
  ;

fragment
REGULAR_ID: SIMPLE_LETTER (SIMPLE_LETTER | '$' | '_' | '#' | '0'..'9')*;

// Grammar

query_block
  : SELECT (selected_element (',' selected_element)*)
    from_clause where_clause? hierarchical_query_clause? group_by_clause? model_clause?
  ;

selected_element
  : REGULAR_ID;

from_clause
    : FROM table_ref
    ;

table_ref
    : table_ref_aux join_clause*
    ;

// Case-insensitivity

fragment SELECT: S E L E C T;
fragment FROM: F R O M;

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];
