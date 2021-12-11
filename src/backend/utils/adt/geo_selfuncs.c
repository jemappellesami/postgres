/*-------------------------------------------------------------------------
 *
 * geo_selfuncs.c
 *	  Selectivity routines registered in the operator catalog in the
 *	  "oprrest" and "oprjoin" attributes.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/geo_selfuncs.c
 *
 *	XXX These are totally bogus.  Perhaps someone will make them do
 *	something reasonable, someday.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/geo_decls.h"
#include "access/htup_details.h"
#include "catalog/pg_statistic.h"
#include "nodes/pg_list.h"
#include "optimizer/pathnode.h"
#include "optimizer/optimizer.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/selfuncs.h"
#include "utils/rangetypes.h"

/*
 *	Selectivity functions for geometric operators.  These are bogus -- unless
 *	we know the actual key distribution in the index, we can't make a good
 *	prediction of the selectivity of these operators.
 *
 *	Note: the values used here may look unreasonably small.  Perhaps they
 *	are.  For now, we want to make sure that the optimizer will make use
 *	of a geometric index if one is available, so the selectivity had better
 *	be fairly small.
 *
 *	In general, GiST needs to search multiple subtrees in order to guarantee
 *	that all occurrences of the same key have been found.  Because of this,
 *	the estimated cost for scanning the index ought to be higher than the
 *	output selectivity would indicate.  gistcostestimate(), over in selfuncs.c,
 *	ought to be adjusted accordingly --- but until we can generate somewhat
 *	realistic numbers here, it hardly matters...
 */


/*
 * Selectivity for operators that depend on area, such as "overlap".
 */

Datum
areasel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.005);
}

Datum
areajoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.005);
}

/*
 *	positionsel
 *
 * How likely is a box to be strictly left of (right of, above, below)
 * a given box?
 */

Datum
positionsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.1);
}

Datum
positionjoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.1);
}

/*
 *	contsel -- How likely is a box to contain (be contained by) a given box?
 *
 * This is a tighter constraint than "overlap", so produce a smaller
 * estimate than areasel does.
 */

Datum
contsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.001);
}

Datum
contjoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.001);
}

/*
 * Range Overlaps Join Selectivity.
 */
Datum
rangeoverlapsjoinsel(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid         operator = PG_GETARG_OID(1);
    List       *args = (List *) PG_GETARG_POINTER(2);
    JoinType    jointype = (JoinType) PG_GETARG_INT16(3);
    SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) PG_GETARG_POINTER(4);
    Oid         collation = PG_GET_COLLATION();

    double      selec = 0.005;
    double      cardinality_estimation = 0; // number of rows that will be estimated

    VariableStatData vardata1;
    VariableStatData vardata2;
    Oid         opfuncoid;
    AttStatsSlot sslot1, 
                    sslot2;
    int         nhist1, nhist2;
    
    Datum     *frequency_hist_smallest_values;
    Datum     *frequency_hist_largest_values;
    int         i;
    Form_pg_statistic stats1 = NULL,
                        stats2 = NULL ;
    TypeCacheEntry *typcache = NULL;
    bool        join_is_reversed;
    bool        empty;


    get_join_variables(root, args, sjinfo,
                       &vardata1, &vardata2, &join_is_reversed);

    typcache = range_get_typcache(fcinfo, vardata1.vartype);
    opfuncoid = get_opcode(operator);

    memset(&sslot1, 0, sizeof(sslot1));
    memset(&sslot2, 0, sizeof(sslot2));

    /* Can't use the histogram with insecure range support functions */
    if (!statistic_proc_security_check(&vardata1, opfuncoid))
        PG_RETURN_FLOAT8((float8) selec);

    if (HeapTupleIsValid(vardata1.statsTuple))
    {
        stats1 = (Form_pg_statistic) GETSTRUCT(vardata1.statsTuple);
        stats2 = (Form_pg_statistic) GETSTRUCT(vardata1.statsTuple);
        /* Try to get fraction of empty ranges */
        if (!get_attstatsslot(&sslot1, vardata1.statsTuple,
                             STATISTIC_KIND_FREQUENCY_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
        if (!get_attstatsslot(&sslot2, vardata2.statsTuple,
                             STATISTIC_KIND_FREQUENCY_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
    }

    nhist1 = sslot1.nvalues;
    nhist2 = sslot2.nvalues;
    
    


    double rows1 = vardata1.rel->rows ;
    double rows2 = vardata2.rel->rows ;
    double cartesian_product_cardinality = rows1*rows2 ;





    double width_1 = DatumGetFloat8(sslot1.values[0]);
    double width_2 = DatumGetFloat8(sslot2.values[0]);
    int nhist_smallest, nhist_largest ;
    double smallest_width ;
    double biggest_width ;
    double smallest_length ;
    double increment ;


    // Identify which histogram is smaller, and fill histograms
    // 1. width_1 > width_2
    if(width_1 > width_2){
        frequency_hist_smallest_values = (Datum *) palloc(sizeof(Datum) * (nhist2-1));
        frequency_hist_largest_values = (Datum *) palloc(sizeof(Datum) * (nhist1-1));
        smallest_width = width_2 ;
        biggest_width = width_1 ;
        nhist_largest = nhist1-1 ;
        nhist_smallest = nhist2-1 ;
        smallest_length = nhist2-1 ;
        increment = 1 ;

        // Histogram filling
        for (i = 0 ; i < nhist2-1 ; i ++){
            frequency_hist_smallest_values[i] = sslot2.values[i+1] ;
        }
        for (i = 0 ; i < nhist1-1 ; i ++){
            frequency_hist_largest_values[i] = sslot1.values[i+1] ;
        }
        // Histogram is filled
    }
    // 2. width_2 > width_1
    else if (width_2 > width_1){
        frequency_hist_smallest_values = (Datum *) palloc(sizeof(Datum) * (nhist1-1));
        frequency_hist_largest_values = (Datum *) palloc(sizeof(Datum) * (nhist2-1));
        smallest_width = width_1 ;
        biggest_width = width_2 ;
        nhist_largest = nhist2-1 ;
        nhist_smallest = nhist1-1 ;
        smallest_length = nhist1-1 ;
        increment = 1 ;

        // Histogram filling
        for (i = 0 ; i < nhist1-1 ; i ++){
            frequency_hist_smallest_values[i] = sslot1.values[i+1] ;
        }
        for (i = 0 ; i < nhist2-1 ; i ++){
            frequency_hist_largest_values[i] = sslot2.values[i+1] ;
        }
        // Histogram is filled

    }
    // 3. width_2 == width_1
    else{
        frequency_hist_smallest_values = (Datum *) palloc(sizeof(Datum) * (nhist1-1));
        frequency_hist_largest_values = (Datum *) palloc(sizeof(Datum) * (nhist2-1));
        smallest_width = width_1 ;
        biggest_width = width_1 ;
        nhist_largest = nhist2-1 ;
        nhist_smallest = nhist1-1 ;
        smallest_length = nhist1-1 ;
        increment = 0 ;

        // Histogram filling
        for (i = 0 ; i < nhist1-1 ; i ++){
            frequency_hist_smallest_values[i] = sslot1.values[i+1] ;
        }
        for (i = 0 ; i < nhist2-1 ; i ++){
            frequency_hist_largest_values[i] = sslot2.values[i+1] ;
        }
        // Histogram is filled
    }

    // the histograms are properly sorted 

    //
    float8 delta = smallest_width/biggest_width ;

    for (int i = 0; i < smallest_length; i++)
    {
        int begin = i*delta ;
        int end = (i+1)* delta ;
        for (int j = begin; j < end + increment; j++)
        {
            double a = DatumGetFloat8(frequency_hist_smallest_values[i]) ;
            double b = DatumGetFloat8(frequency_hist_largest_values[j]) ;
            double c = a*b ;
            cardinality_estimation += c ;
        }
        
    }
    printf("Join cardinality estimation without post-processing : %f \n\n", cardinality_estimation) ;
    

    double average1 ;
    double average2 ;
    double total_sum = 0 ;
    double nb_zeros = 0 ;

    for (int i = 0; i < nhist_smallest; i++)
    {
        total_sum += DatumGetFloat8(frequency_hist_smallest_values[i]) ;
        if (!DatumGetFloat8(frequency_hist_smallest_values[i]))
        {
            nb_zeros ++ ;
        }
    }

    average1 = total_sum/(nhist_smallest-nb_zeros) ;

    total_sum = nb_zeros = 0 ;    
    for (int i = 0; i < nhist_largest; i++)
    {
        total_sum += DatumGetFloat8(frequency_hist_largest_values[i]) ;
        if (!DatumGetFloat8(frequency_hist_largest_values[i]))
        {
            nb_zeros ++ ;
        }
    }
    average2 = total_sum/(nhist_smallest-nb_zeros) ;


    double dampening_factor1 = log(average1) * log(average2)/sqrt(log((nhist_smallest + nhist_largest)/2)) ;
    printf("Cardinality estimation 1 : %f \n", cardinality_estimation/dampening_factor1) ;

    double dampening_factor2 = sqrt(pow(average1, 2) + pow(average2, 2)) ;
    printf("Cardinality estimation 2 : %f \n", cardinality_estimation/dampening_factor2) ;

    // double dampening_factor1 = log(average1 * average2)/sqrt((average1*average2)) ;
    // printf("Cardinality estimation 1 : %f \n", cardinality_estimation/dampening_factor1) ;


    //Print of frequency histograms
    // printf("hist_frequency_1 = [");
    // for (i = 0; i < nhist1-1; i++)
    // {
    //     double frequency = DatumGetFloat8(frequency_hist_smallest_values[i]) ;
    //     printf("%f", frequency) ;
    //     if (i < nhist1 - 2)
    //         printf(", ");
    // }
    // printf("]\n\n");
    // printf("hist_frequency_2 = [");
    // for (i = 0; i < nhist2-1; i++)
    // {
    //     double frequency = DatumGetFloat8(frequency_hist_largest_values[i]) ;
    //     printf("%f", frequency) ;
    //     if (i < nhist2 - 2)
    //         printf(", ");
    // }
    // printf("]\n");

    fflush(stdout);

    // Result : on a bien les largeurs en index 0

    pfree(frequency_hist_smallest_values);
    pfree(frequency_hist_largest_values);

    free_attstatsslot(&sslot1);
    free_attstatsslot(&sslot2);

    ReleaseVariableStats(vardata1);
    ReleaseVariableStats(vardata2);

    CLAMP_PROBABILITY(selec);
    PG_RETURN_FLOAT8((float8) selec);
}


Datum
rangeoverlapsjoinsel_prof(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid         operator = PG_GETARG_OID(1);
    List       *args = (List *) PG_GETARG_POINTER(2);
    JoinType    jointype = (JoinType) PG_GETARG_INT16(3);
    SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) PG_GETARG_POINTER(4);
    Oid         collation = PG_GET_COLLATION();

    double      selec = 0.005;

    VariableStatData vardata1;
    VariableStatData vardata2;
    Oid         opfuncoid;
    AttStatsSlot sslot1, 
                    sslot2;
    int         nhist1, nhist2;
    RangeBound *hist_lower1;
    RangeBound *hist_upper1;
    Datum     *length_hist_values;
    int         i;
    Form_pg_statistic stats1 = NULL,
                        stats2 = NULL ;
    TypeCacheEntry *typcache = NULL;
    bool        join_is_reversed;
    bool        empty;


    get_join_variables(root, args, sjinfo,
                       &vardata1, &vardata2, &join_is_reversed);

    typcache = range_get_typcache(fcinfo, vardata1.vartype);
    opfuncoid = get_opcode(operator);

    memset(&sslot1, 0, sizeof(sslot1));
    memset(&sslot2, 0, sizeof(sslot2));

    /* Can't use the histogram with insecure range support functions */
    if (!statistic_proc_security_check(&vardata1, opfuncoid))
        PG_RETURN_FLOAT8((float8) selec);

    if (HeapTupleIsValid(vardata1.statsTuple))
    {
        stats1 = (Form_pg_statistic) GETSTRUCT(vardata1.statsTuple);
        stats2 = (Form_pg_statistic) GETSTRUCT(vardata1.statsTuple);
        /* Try to get fraction of empty ranges */
        if (!get_attstatsslot(&sslot1, vardata1.statsTuple,
                             STATISTIC_KIND_BOUNDS_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
        if (!get_attstatsslot(&sslot2, vardata1.statsTuple,
                             STATISTIC_KIND_FREQUENCY_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
    }

    nhist1 = sslot1.nvalues;
    nhist2 = sslot2.nvalues;
    hist_lower1 = (RangeBound *) palloc(sizeof(RangeBound) * nhist1);
    hist_upper1 = (RangeBound *) palloc(sizeof(RangeBound) * nhist1);
    length_hist_values = (Datum *) palloc(sizeof(Datum) * nhist2);
    for (i = 0; i < nhist1; i++)
    {
        range_deserialize(typcache, DatumGetRangeTypeP(sslot1.values[i]),
                          &hist_lower1[i], &hist_upper1[i], &empty);
        /* The histogram should not contain any empty ranges */
        if (empty)
            elog(ERROR, "bounds histogram contains an empty range");
    }

    for (i = 0 ; i < nhist2 ; i ++){
        length_hist_values[i] = sslot2.values[i] ;
    }


    // printf("hist_lower = [");
    // for (i = 0; i < nhist1; i++)
    // {
    //     printf("%d", DatumGetInt16(hist_lower1[i].val));
    //     if (i < nhist1 - 1)
    //         printf(", ");
    // }
    // printf("]\n");
    // printf("hist_upper = [");
    // for (i = 0; i < nhist1; i++)
    // {
    //     printf("%d", DatumGetInt16(hist_upper1[i].val));
    //     if (i < nhist1 - 1)
    //         printf(", ");
    // }
    // printf("]\n");

    printf("hist_length = [");
    for (i = 0; i < nhist2; i++)
    {
        double length = DatumGetFloat8(length_hist_values[i]) ;
        printf("%f", length) ;
        if (i < nhist2 - 1)
            printf(", ");
    }
    printf("]\n");

    fflush(stdout);

    // pfree(hist_lower1);
    // pfree(hist_upper1);
    pfree(length_hist_values);

    free_attstatsslot(&sslot1);
    free_attstatsslot(&sslot2);

    ReleaseVariableStats(vardata1);
    ReleaseVariableStats(vardata2);

    CLAMP_PROBABILITY(selec);
    PG_RETURN_FLOAT8((float8) selec);
}

