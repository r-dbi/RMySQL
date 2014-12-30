/*
 * ----
 * The name of the project is RMySQL, not RSMySQL, so at some point we
 * will inevitably break compatibility with Splus.
 *
 * FAIR WARNING!
 *
 * Jeff Horner Wed Jan 28 10:14:02 CST 2009
 * ----
 *
 * S4 (Splus5+) and R portability macros.
 *
 * This file provides additional macros to the ones in Rdefines.h (in R)
 * and S4/Splus5 S.h (see Appendix A of the green book) to
 * allow portability between R > 1.0.0, S4, and Splus5+ at the C source
 * level.  In addition to the macros in Rdefines.h and Appendix A,
 * we have macros to do x[[i][j] and x[[i]][j] <- val inside C functions,
 * macros to test for primitive data types, plus macros to test and
 * set NA's portably.
 * TODO: Macros to build and eval functions portably?
 */

#ifndef S4R_H
#define S4R_H

#ifdef __cplusplus
extern "C" {
#endif

#  include "S.h"
#  include "Rdefines.h"

/* We simplify one- and two-level access to object and list
 * (mostly built on top of jmc's macros)
 *
 * NOTE: Recall that list element vectors should *not* be set
 * directly, but only thru SET_ELEMENT (Green book, Appendix A), e.g.,
 *      LIST_POINTER(x)[i] = NEW_CHARACTER(100);    BAD!!
 *      LST_EL(x,i) = NEW_CHARACTER(100);           BAD!!
 *      SET_ELEMENT(x, i, NEW_CHARACTER(100));      Okay
 *
 * It's okay to directly set the i'th element of the j'th list element:
 *      LST_CHR_EL(x,i,j) = C_S_CPY(str);           Okay (but not in R-1.2.1)
 *
 * For R >= 1.2.0 define
 *      SET_LST_CHR_EL(x,i,j,val)
 */

/* x[i] */
#define INT_EL(x,i) INTEGER((x))[(i)]
#define NUM_EL(x,i) REAL((x))[(i)]
#define LST_EL(x,i) VECTOR_ELT((x),(i))
#define CHR_EL(x,i) CHAR(STRING_ELT((x),(i)))
#define SET_CHR_EL(x,i,val)  SET_STRING_ELT((x),(i), (val))

/* x[[i]][j] -- can be also assigned if x[[i]] is a numeric type */
#define LST_CHR_EL(x,i,j) CHR_EL(LST_EL((x),(i)), (j))
#define LST_INT_EL(x,i,j) INT_EL(LST_EL((x),(i)), (j))
#define LST_NUM_EL(x,i,j) NUM_EL(LST_EL((x),(i)), (j))

/* x[[i]][j] -- for the case when x[[i]] is a character type */
#define SET_LST_CHR_EL(x,i,j,val) SET_STRING_ELT(LST_EL(x,i), j, val)

/* setting and querying NA's -- in the case of R, we need to
 * use our own RS_na_set and RS_is_na functions (these need work!)
 */

#  define NA_SET(p,t)   RS_na_set((p),(t))
#  define NA_CHR_SET(p) SET_CHR_EL(p, 0, NA_STRING)
#  define IS_NA(p,t)    RS_is_na((p),(t))

/* end of RS-DBI macros */

#ifdef __cplusplus
}
#endif

#endif /* S4R_H */
