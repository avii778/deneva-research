from experiments import *
import pprint

YCSB_SKEW_THRESHOLD = None

def plot_all():
    return 0

def _ycsb_scaling_dimension_plot(summary,summary_cl,x_name,fixed_name,
                                 xlab,name_suffix,logscalex=False,
                                 experiment_generator=None,
                                 x_divisor=None,x_max=None):
    """Plot one YCSB scaling dimension while holding the other constant."""
    from experiments import ycsb_scaling, apply_algo_thread_counts
    from helper import plot_prep
    from plot_helper import tput, latency, abort_rate, time_breakdown_line

    if experiment_generator is None:
        experiment_generator = ycsb_scaling
    nfmt,nexp = apply_algo_thread_counts(*experiment_generator())
    v_name = "CC_ALG"
    fixed_vals = sorted(set(row[nfmt.index(fixed_name)] for row in nexp))

    for fixed_val in fixed_vals:
        x_vals,v_vals,fmt,exp,lst = plot_prep(
            nexp,nfmt,x_name,v_name,constants={fixed_name:fixed_val}
        )
        if x_max is not None:
            x_vals = [value for value in x_vals if float(value) <= x_max]
            if not x_vals:
                print("No {} values at or below threshold {}".format(
                    x_name,x_max))
                continue
        fixed_label = str(fixed_val).replace(".","p")
        title = "YCSB scaling, {}={}".format(fixed_name,fixed_val)
        common = {
            "cfg_fmt": fmt,
            "cfg": list(exp),
            "xname": x_name,
            "vname": v_name,
            "title": title,
            "xlab": xlab,
            "new_cfgs": lst,
            "logscalex": logscalex,
            "x_divisor": fixed_val if x_divisor == fixed_name else 1,
        }
        tput(x_vals,v_vals,summary,summary_cl,
             name="tput_ycsb_scaling_{}_{}".format(name_suffix,fixed_label),**common)
        latency(x_vals,v_vals,summary,summary_cl,
                name="latency_ycsb_scaling_{}_{}".format(name_suffix,fixed_label),
                milliseconds=True,**common)
        abort_rate(x_vals,v_vals,summary,summary_cl,
                   name="aborts_ycsb_scaling_{}_{}".format(name_suffix,fixed_label),**common)
        time_breakdown_line(x_vals,v_vals,summary,
                            name="time_break_line_ycsb_scaling_{}_{}".format(name_suffix,fixed_label),**common)

def ycsb_scaling_skew_plot(summary,summary_cl):
    """Plot Zipf skew on the x-axis, with one plot per server count."""
    _ycsb_scaling_dimension_plot(
        summary,summary_cl,"ZIPF_THETA","NODE_CNT","Zipf Theta","skew_nodes",
        x_max=YCSB_SKEW_THRESHOLD
    )

def ycsb_scaling_nodes_plot(summary,summary_cl):
    """Plot server count on the x-axis, with one plot per skew value."""
    _ycsb_scaling_dimension_plot(
        summary,summary_cl,"NODE_CNT","ZIPF_THETA","Server Count","nodes_skew",
        logscalex=True
    )

def ycsb_scaling_uniform_plot(summary,summary_cl):
    """Plot uniform-distribution results by server count."""
    from experiments import ycsb_scaling_uniform
    _ycsb_scaling_dimension_plot(
        summary,summary_cl,"NODE_CNT","ZIPF_THETA","Server Count","nodes_skew",
        logscalex=True,experiment_generator=ycsb_scaling_uniform
    )

def ycsb_scaling_life_fairness_comparison_plot(summary,summary_cl):
    """Compare LIFE throughput and latency with fairness on and off."""
    from experiments import (ycsb_scaling_life_fairness_comparison,
                             apply_algo_thread_counts)
    from helper import plot_prep
    from plot_helper import tput, latency, abort_rate, time_breakdown_line

    nfmt,nexp = apply_algo_thread_counts(
        *ycsb_scaling_life_fairness_comparison())
    x_name = "NODE_CNT"
    v_name = "life_fairness"
    x_vals,v_vals,fmt,exp,lst = plot_prep(
        nexp,nfmt,x_name,v_name,constants={})
    common = {
        "cfg_fmt": fmt,
        "cfg": list(exp),
        "xname": x_name,
        "vname": v_name,
        "title": "LIFE fairness comparison",
        "xlab": "Server Count",
        "new_cfgs": lst,
        "logscalex": len(x_vals) > 1,
    }
    tput(x_vals,v_vals,summary,summary_cl,
         name="tput_ycsb_life_fairness",**common)
    latency(x_vals,v_vals,summary,summary_cl,
            name="latency_ycsb_life_fairness",milliseconds=True,**common)
    abort_rate(x_vals,v_vals,summary,summary_cl,
               name="aborts_ycsb_life_fairness",**common)
    time_breakdown_line(x_vals,v_vals,summary,
                        name="time_break_line_ycsb_life_fairness",**common)

def ycsb_scaling_calvin_prelock_plot(summary,summary_cl):
    """Plot Calvin pre-lock results by server count."""
    from experiments import ycsb_scaling_calvin_prelock
    _ycsb_scaling_dimension_plot(
        summary,summary_cl,"NODE_CNT","ZIPF_THETA","Server Count",
        "calvin_prelock_nodes_skew",logscalex=True,
        experiment_generator=ycsb_scaling_calvin_prelock
    )

def ycsb_scaling_table_size_plot(summary,summary_cl):
    """Plot YCSB performance by per-node table size for each server count."""
    _ycsb_scaling_dimension_plot(
        summary,summary_cl,"SYNTH_TABLE_SIZE","NODE_CNT","Table Size per Node",
        "table_size_nodes",logscalex=True,x_divisor="NODE_CNT"
    )

def ycsb_ppt_plot(summary,summary_cl):
    """Plot throughput against partitions per transaction for each algorithm."""
    from experiments import ycsb_scaling, apply_algo_thread_counts
    from helper import plot_prep
    from plot_helper import tput

    nfmt,nexp = apply_algo_thread_counts(*ycsb_scaling())
    x_name = "PART_PER_TXN"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={})
    tput(
        x_vals,v_vals,summary,summary_cl,
        cfg_fmt=fmt,
        cfg=list(exp),
        xname=x_name,
        vname=v_name,
        title="",
        name="tput_ycsb_ppt",
        xlab="Partitions per Transaction",
        new_cfgs=lst,
    )

def ycsb_scaling_inflight_plot(summary,summary_cl):
    """Plot YCSB performance while varying the transaction in-flight limit."""
    from experiments import ycsb_scaling, apply_algo_thread_counts
    from helper import plot_prep
    from plot_helper import tput, latency, abort_rate, time_breakdown_line

    nfmt,nexp = apply_algo_thread_counts(*ycsb_scaling())
    x_name = "MAX_TXN_IN_FLIGHT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(
        nexp,nfmt,x_name,v_name,constants={}
    )
    common = {
        "cfg_fmt": fmt,
        "cfg": list(exp),
        "xname": x_name,
        "vname": v_name,
        "title": "",
        "xlab": "Maximum Transactions in Flight",
        "new_cfgs": lst,
    }
    tput(x_vals,v_vals,summary,summary_cl,
         name="tput_ycsb_scaling_inflight",**common)
    latency(x_vals,v_vals,summary,summary_cl,
            name="latency_ycsb_scaling_inflight",milliseconds=True,**common)
    abort_rate(x_vals,v_vals,summary,summary_cl,
               name="aborts_ycsb_scaling_inflight",**common)
    time_breakdown_line(x_vals,v_vals,summary,
                        name="time_break_line_ycsb_scaling_inflight",**common)

def ycsb_scaling_req_plot(summary,summary_cl):
    """Plot YCSB performance as the number of requests per query increases."""
    from experiments import ycsb_scaling_req, apply_algo_thread_counts
    from helper import plot_prep
    from plot_helper import tput, latency, abort_rate, time_breakdown_line

    nfmt,nexp = apply_algo_thread_counts(*ycsb_scaling_req())
    x_name = "REQ_PER_QUERY"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(
        nexp,nfmt,x_name,v_name,constants={}
    )
    common = {
        "cfg_fmt": fmt,
        "cfg": list(exp),
        "xname": x_name,
        "vname": v_name,
        "title": "",
        "xlab": "Requests per Transaction",
        "new_cfgs": lst,
    }
    tput(x_vals,v_vals,summary,summary_cl,
         name="tput_ycsb_scaling_req",**common)
    latency(x_vals,v_vals,summary,summary_cl,
            name="latency_ycsb_scaling_req",milliseconds=True,**common)
    abort_rate(x_vals,v_vals,summary,summary_cl,
               name="aborts_ycsb_scaling_req",**common)
    time_breakdown_line(x_vals,v_vals,summary,
                        name="time_break_line_ycsb_scaling_req",**common)

def ycsb_scaling_current_plot(summary,summary_cl):
    """Plot the dimension varied by the current ycsb_scaling sweep."""
    ycsb_scaling_table_size_plot(summary,summary_cl)

def ppr_ycsb_scaling_plot(summary,summary_cl):
    from experiments import ycsb_scaling
    from helper import plot_prep
    from plot_helper import tput,time_breakdown,latency,abort_rate,latency_breakdown,time_breakdown_line
    nfmt,nexp = ycsb_scaling()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.0,"ZIPF_THETA":0.0})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_readonly",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.0,"ZIPF_THETA":0.0})
    latency(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="latency_ycsb_scaling_readonly",xlab="Server Count",new_cfgs=lst,logscalex=True,milliseconds=True)
    nfmt,nexp = ycsb_scaling()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"NODE_CNT":16,"TXN_WRITE_PERC":0.0,"ZIPF_THETA":0.0})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_readonly',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)

    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_med",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    latency(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="latency_ycsb_scaling_med",xlab="Server Count",new_cfgs=lst,logscalex=True,milliseconds=True)
    nfmt,nexp = ycsb_scaling()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    time_breakdown_line(x_vals,v_vals,summary,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="time_break_line_ycsb_scaling_med",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    abort_rate(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="aborts_ycsb_scaling_med",xlab="Server Count",new_cfgs=lst,logscalex=True)
    nfmt,nexp = ycsb_scaling()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"NODE_CNT":16,"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_med',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)
    nfmt,nexp = ycsb_scaling()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"NODE_CNT":16,"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    latency_breakdown(x_vals,summary,xname=x_name,title='',name='latency_breakdown_ycsb_scaling_med',cfg_fmt=fmt,cfg=list(exp),normalized=False,new_cfgs=lst)


    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.7})
    time_breakdown_line(x_vals,v_vals,summary,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="time_break_line_ycsb_scaling_high",xlab="Server Count",new_cfgs=lst,logscalex=True)
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_high",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.7})
    latency(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="latency_ycsb_scaling_high",xlab="Server Count",new_cfgs=lst,logscalex=True,milliseconds=True)
    abort_rate(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="aborts_ycsb_scaling_high",xlab="Server Count",new_cfgs=lst,logscalex=True)
    nfmt,nexp = ycsb_scaling()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"NODE_CNT":16,"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.7})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_high',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)

def ycsb_single_node_plot(summary,summary_cl):
    from experiments import ycsb_single_node
    from helper import plot_prep
    from plot_helper import tput,latency,abort_rate
    nfmt,nexp = ycsb_single_node()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"TXN_WRITE_PERC":0.0,"ZIPF_THETA":0.0})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname='',title="",name="tput_ycsb_single_node_readonly",new_cfgs=lst)
    latency(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname='',title="",name="latency_ycsb_single_node_readonly",new_cfgs=lst)
    nfmt,nexp = ycsb_single_node()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname='',title="",name="tput_ycsb_single_node_med",new_cfgs=lst)
    latency(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname='',title="",name="latency_ycsb_single_node_med",new_cfgs=lst)
    abort_rate(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname='',title="",name="aborts_ycsb_single_node_med",new_cfgs=lst)

def ycsb_single_node_writes_plot(summary,summary_cl):
    from experiments import ycsb_single_node_writes
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_single_node_writes()
    x_name = "TXN_WRITE_PERC"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":1,"ZIPF_THETA":0.6})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_single_node_writes",xlab="% of Update Transactions",new_cfgs=lst)

def ppr_pps_scaling_plot(summary,summary_cl):
    from experiments import pps_scaling
    from helper import plot_prep
    from plot_helper import tput,time_breakdown,latency,abort_rate,latency_breakdown
    nfmt,nexp = pps_scaling()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_pps_scaling",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={})
    latency(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="latency_pps_scaling",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={})
    abort_rate(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="aborts_pps_scaling",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"NODE_CNT":16})
    latency_breakdown(x_vals,summary,xname=x_name,title='',name='latency_breakdown_pps_scaling',cfg_fmt=fmt,cfg=list(exp),normalized=False,new_cfgs=lst)

def ppr_ecwc_plot(summary,summary_cl):
    from experiments import ecwc
    from helper import plot_prep
    from plot_helper import tput,time_breakdown,latency,abort_rate,latency_breakdown,tput_stack
    nfmt,nexp = ecwc()
    x_name = "CC_ALG"
    v_name = ""
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    tput_stack(x_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,title="",name="tput_ecwc",new_cfgs=lst)

def ppr_ycsb_scaling_abort_plot(summary,summary_cl):
    from experiments import ycsb_scaling_abort
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = ycsb_scaling_abort()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6,"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_abort_med",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.7,"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_abort_high",xlab="Server Count",new_cfgs=lst,logscalex=True)


def ppr_tpcc_scaling_plot(summary,summary_cl):
    from experiments import tpcc_scaling
    from helper import get_cfgs,get_outfile_name,plot_prep
    from plot_helper import tput
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    nfmt,nexp = tpcc_scaling()
    node_idx = nfmt.index("NODE_CNT")
    warehouse_idx = nfmt.index("NUM_WH")

    # tpcc_scaling contains two warehouse regimes for the same node/algo
    # coordinates. Plot them separately so plot_prep's (x, series) lookup does
    # not silently let the later regime overwrite the earlier one.
    for warehouses_per_server in (4,128):
        regime = [
            row for row in nexp
            if row[warehouse_idx] == warehouses_per_server * row[node_idx]
        ]
        for payment_fraction,workload_name in ((0.0,"neworder"),(1.0,"payment")):
            selected = [
                row for row in regime
                if row[nfmt.index("PERC_PAYMENT")] == payment_fraction
            ]

            # A missing run must not become a plausible-looking zero in the
            # output. Validate both server and client summaries before plotting.
            missing = []
            for row in selected:
                result_name = get_outfile_name(get_cfgs(nfmt,row),nfmt)
                if result_name not in summary or result_name not in summary_cl:
                    missing.append(result_name)
            if missing:
                raise RuntimeError(
                    "Missing TPCC scaling summaries: {}".format(", ".join(missing))
                )

            x_vals,v_vals,fmt,exp,lst = plot_prep(
                selected,nfmt,x_name,v_name,constants={}
            )
            tput(
                x_vals,v_vals,summary,summary_cl,
                cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,
                title="{} warehouses per server".format(warehouses_per_server),
                name="tput_tpcc_{}_{}wh_per_server".format(
                    workload_name,warehouses_per_server
                ),
                xlab="Server Count",logscalex=True,new_cfgs=lst
            )

def ppr_ycsb_partitions_plot(summary,summary_cl):
    from experiments import ycsb_partitions,ycsb_partitions_distr
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_partitions()
    x_name = "PART_PER_TXN"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_partitions",xlab="Partitions Accessed",new_cfgs=lst)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"MAX_TXN_IN_FLIGHT":12000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_partitions_12k",xlab="Partitions Accessed",new_cfgs=lst)

    nfmt,nexp = ycsb_partitions_distr()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_partitions_distr",xlab="Partitions Accessed",new_cfgs=lst)

def ppr_ycsb_partitions_abort_plot(summary,summary_cl):
    from experiments import ycsb_partitions_abort
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_partitions_abort()
    x_name = "PART_PER_TXN"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_partitions_abort",xlab="Partitions Accessed",new_cfgs=lst)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"MAX_TXN_IN_FLIGHT":12000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_partitions_12k",xlab="Partitions Accessed",new_cfgs=lst)



def ppr_ycsb_writes_plot(summary,summary_cl):
    from experiments import ycsb_writes, apply_algo_thread_counts
    from helper import plot_prep
    from plot_helper import tput, latency, abort_rate, time_breakdown_line
    nfmt,nexp = apply_algo_thread_counts(*ycsb_writes())
    x_name = "TXN_WRITE_PERC"
    v_name = "CC_ALG"

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16,"ZIPF_THETA":0.3,"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_writes_16",xlab="% of Update Transactions",new_cfgs=lst)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16,"ZIPF_THETA":0.3,"MAX_TXN_IN_FLIGHT":10000})
    latency(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="latency_ycsb_writes_16",xlab="% of Update Transactions",new_cfgs=lst,milliseconds=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16,"ZIPF_THETA":0.3,"MAX_TXN_IN_FLIGHT":10000})
    abort_rate(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="aborts_ycsb_writes_16",xlab="% of Update Transactions",new_cfgs=lst)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16,"ZIPF_THETA":0.3,"MAX_TXN_IN_FLIGHT":10000})
    time_breakdown_line(x_vals,v_vals,summary,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="time_break_line_ycsb_writes_16",xlab="% of Update Transactions",new_cfgs=lst)

def ppr_ycsb_skew_abort_plot(summary,summary_cl):
    from experiments import ycsb_skew_abort   
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_skew_abort()
    x_name = "ZIPF_THETA"
    v_name = "CC_ALG"

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_skew_abort_16",xlab="Skew Factor (Theta)",new_cfgs=lst)

    x_name = "NODE_CNT"
    nfmt,nexp = ycsb_skew_abort()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_abort",xlab="Server Count",new_cfgs=lst,logscalex=True)



def ppr_ycsb_skew_plot(summary,summary_cl):
    from experiments import ycsb_skew   
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_skew()
    x_name = "ZIPF_THETA"
    v_name = "CC_ALG"

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":2})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_skew_2",xlab="Zipf Theta",new_cfgs=lst,ylimit=120)

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":4})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_skew_4",xlab="Zipf Theta",new_cfgs=lst,ylimit=120)

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":8})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_skew_8",xlab="Zipf Theta",new_cfgs=lst,ylimit=120)

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_skew_16",xlab="Skew Factor (Theta)",new_cfgs=lst)

def ppr_isolation_levels_plot(summary,summary_cl):
    from experiments import isolation_levels 
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = isolation_levels()
    x_name = "NODE_CNT"
    v_name = "ISOLATION_LEVEL"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={'ZIPF_THETA':0.6})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_gold",xlab="Server Count",logscalex=True,new_cfgs=lst,legend=True)

def ppr_network_plot(summary,summary_cl):
    from experiments import network_sweep
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = network_sweep()
    v_name = "CC_ALG"
    x_name = "NETWORK_DELAY"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6,"NODE_CNT":2})
    x_vals = [float(v)/1000 for v in x_vals]
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_network",xlab="Network Latency (ms)",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6,"NODE_CNT":8})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_network_8",xlab="Network Latency (ms)",new_cfgs=lst,logscalex=True)
