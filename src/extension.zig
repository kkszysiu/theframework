const py = @cImport({
    @cInclude("py_helpers.h");
});
const hub = @import("hub.zig");
const lazy_request = @import("lazy_request.zig");

const PyObject = py.PyObject;
const PyMethodDef = py.PyMethodDef;
const PyModuleDef = py.PyModuleDef;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn incref(o: ?*PyObject) void {
    if (o) |obj| py.py_helper_incref(obj);
}

fn decref(o: ?*PyObject) void {
    if (o) |obj| py.py_helper_decref(obj);
}

fn pyNone() ?*PyObject {
    const none = py.py_helper_none();
    incref(none);
    return none;
}

// ---------------------------------------------------------------------------
// Module methods
// ---------------------------------------------------------------------------

/// hello() -> str
fn hello(_: ?*PyObject, _: ?*PyObject) callconv(.c) ?*PyObject {
    return py.PyUnicode_FromString("hello from Zig");
}

/// greenlet_switch_to(callable) -> result
///
/// Create a new greenlet running `callable`, switch to it, and return
/// whatever the greenlet returns (i.e. the value it passes back when
/// it finishes or explicitly switches to its parent).
fn greenletSwitchTo(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var callable: ?*PyObject = null;
    if (py.PyArg_ParseTuple(args, "O", &callable) == 0)
        return null;

    // Get current greenlet (the caller) — it will be the parent.
    const current = py.py_helper_greenlet_getcurrent() orelse return null;
    defer decref(current);

    // Create a new greenlet: greenlet(callable, parent=current)
    const g = py.py_helper_greenlet_new(callable, current) orelse return null;
    defer decref(g);

    // Switch to the new greenlet with no args.
    // It will run callable(). When callable returns, the return value
    // arrives here as the result of the switch.
    const result = py.py_helper_greenlet_switch(g, null, null);
    return result;
}

/// greenlet_ping_pong(callable, n) -> list
///
/// Create a greenlet running `callable`. Switch to it `n` times, each
/// time passing an incrementing counter. The callable is expected to
/// switch back to its parent with a transformed value each time.
/// Returns a list of all values received.
fn greenletPingPong(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var callable: ?*PyObject = null;
    var n: c_int = 0;
    if (py.PyArg_ParseTuple(args, "Oi", &callable, &n) == 0)
        return null;

    const current = py.py_helper_greenlet_getcurrent() orelse return null;
    defer decref(current);

    const g = py.py_helper_greenlet_new(callable, current) orelse return null;
    defer decref(g);

    const results = py.PyList_New(0) orelse return null;

    // First switch starts the greenlet (no args needed).
    var switch_args = py.py_helper_tuple_new(1) orelse {
        decref(results);
        return null;
    };
    const zero = py.PyLong_FromLong(0);
    _ = py.py_helper_tuple_setitem(switch_args, 0, zero);

    var val = py.py_helper_greenlet_switch(g, switch_args, null);
    decref(switch_args);

    if (val == null) {
        decref(results);
        return null;
    }

    _ = py.PyList_Append(results, val);
    decref(val);

    // Subsequent switches pass the counter.
    var i: c_int = 1;
    while (i < n) : (i += 1) {
        if (py.py_helper_greenlet_active(g) == 0) break;

        switch_args = py.py_helper_tuple_new(1) orelse {
            decref(results);
            return null;
        };
        const counter = py.PyLong_FromLong(@as(c_long, i));
        _ = py.py_helper_tuple_setitem(switch_args, 0, counter);

        val = py.py_helper_greenlet_switch(g, switch_args, null);
        decref(switch_args);

        if (val == null) {
            // Exception in the greenlet
            decref(results);
            return null;
        }
        _ = py.PyList_Append(results, val);
        decref(val);
    }

    return results;
}

/// run_hub(workers: list[greenlet]) -> list
///
/// Run a Zig-implemented hub that round-robins between the given worker
/// greenlets. Each worker must switch back to the hub (its parent) to
/// yield. The hub collects every value yielded by workers and returns
/// them as a list in the order received.
///
/// Workers are expected to be greenlet objects whose parent is the
/// current (calling) greenlet — which becomes the hub.
fn runHub(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var workers_list: ?*PyObject = null;
    if (py.PyArg_ParseTuple(args, "O", &workers_list) == 0)
        return null;

    const num_workers = py.PyList_Size(workers_list orelse return null);
    if (num_workers < 0) return null;

    const results = py.PyList_New(0) orelse return null;

    // Hub loop: round-robin until all workers are dead.
    var all_done = false;
    while (!all_done) {
        all_done = true;
        var i: isize = 0;
        while (i < num_workers) : (i += 1) {
            const g = py.PyList_GetItem(workers_list, i) orelse {
                decref(results);
                return null;
            };
            // Skip dead greenlets
            if (py.py_helper_greenlet_started(g) != 0 and py.py_helper_greenlet_active(g) == 0)
                continue;

            all_done = false;

            // Switch to the worker. It will run until it yields back to us.
            const val = py.py_helper_greenlet_switch(g, null, null);
            if (val == null) {
                // Exception propagated from the worker.
                // Check if the exception should be swallowed or propagated.
                if (py.py_helper_err_occurred() != null) {
                    decref(results);
                    return null;
                }
                continue;
            }

            // If the worker yielded a value (not None from finishing), collect it.
            if (val != py.py_helper_none()) {
                _ = py.PyList_Append(results, val);
            }
            decref(val);
        }
    }

    return results;
}

// ---------------------------------------------------------------------------
// Method table and module definition (runtime-initialized because
// sentinels and flag constants come from extern C helpers)
// ---------------------------------------------------------------------------

var methods: [25]PyMethodDef = undefined;
var module_def: PyModuleDef = undefined;

// ---------------------------------------------------------------------------
// Module init — called by Python when `import _framework_core` runs
// ---------------------------------------------------------------------------

pub export fn PyInit__framework_core() callconv(.c) ?*PyObject {
    // Initialize greenlet C API
    if (py.py_helper_greenlet_import() != 0) return null;

    methods[0] = .{
        .ml_name = "hello",
        .ml_meth = @ptrCast(&hello),
        .ml_flags = py.py_helper_meth_noargs(),
        .ml_doc = "Return a greeting from Zig.",
    };
    methods[1] = .{
        .ml_name = "greenlet_switch_to",
        .ml_meth = @ptrCast(&greenletSwitchTo),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Create a greenlet running callable, switch to it, return its result.",
    };
    methods[2] = .{
        .ml_name = "greenlet_ping_pong",
        .ml_meth = @ptrCast(&greenletPingPong),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Ping-pong with a greenlet n times, returning a list of results.",
    };
    methods[3] = .{
        .ml_name = "run_hub",
        .ml_meth = @ptrCast(&runHub),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Run a Zig hub that round-robins between worker greenlets.",
    };
    methods[4] = .{
        .ml_name = "hub_run",
        .ml_meth = @ptrCast(&hub.pyHubRun),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Init hub, create acceptor greenlet, run hub loop.",
    };
    methods[5] = .{
        .ml_name = "green_accept",
        .ml_meth = @ptrCast(&hub.pyGreenAccept),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Submit accept, yield to hub, return new fd.",
    };
    methods[6] = .{
        .ml_name = "green_recv",
        .ml_meth = @ptrCast(&hub.pyGreenRecv),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Submit recv, yield to hub, return data as bytes.",
    };
    methods[7] = .{
        .ml_name = "green_send",
        .ml_meth = @ptrCast(&hub.pyGreenSend),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Submit send, yield to hub, return bytes sent.",
    };
    methods[8] = .{
        .ml_name = "hub_stop",
        .ml_meth = @ptrCast(&hub.pyHubStop),
        .ml_flags = py.py_helper_meth_noargs(),
        .ml_doc = "Thread-safe stop signal for the hub.",
    };
    methods[9] = .{
        .ml_name = "hub_schedule",
        .ml_meth = @ptrCast(&hub.pyHubSchedule),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Add greenlet to hub ready queue.",
    };
    methods[10] = .{
        .ml_name = "get_hub_greenlet",
        .ml_meth = @ptrCast(&hub.pyGetHubGreenlet),
        .ml_flags = py.py_helper_meth_noargs(),
        .ml_doc = "Get the hub greenlet object.",
    };
    methods[11] = .{
        .ml_name = "green_close",
        .ml_meth = @ptrCast(&hub.pyGreenClose),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Close fd via io_uring, cleaning up connection pool.",
    };
    methods[12] = .{
        .ml_name = "green_sleep",
        .ml_meth = @ptrCast(&hub.pyGreenSleep),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Sleep cooperatively for the given number of seconds.",
    };
    methods[13] = .{
        .ml_name = "green_connect",
        .ml_meth = @ptrCast(&hub.pyGreenConnect),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Create socket, connect to host:port cooperatively, return fd.",
    };
    methods[14] = .{
        .ml_name = "green_register_fd",
        .ml_meth = @ptrCast(&hub.pyGreenRegisterFd),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Register an external fd in the connection pool.",
    };
    methods[15] = .{
        .ml_name = "green_unregister_fd",
        .ml_meth = @ptrCast(&hub.pyGreenUnregisterFd),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Remove fd from connection pool without closing it.",
    };
    methods[16] = .{
        .ml_name = "green_connect_fd",
        .ml_meth = @ptrCast(&hub.pyGreenConnectFd),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Connect an already-registered fd cooperatively.",
    };
    methods[17] = .{
        .ml_name = "green_poll_fd",
        .ml_meth = @ptrCast(&hub.pyGreenPollFd),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Wait for fd readiness via POLL_ADD, return revents.",
    };
    methods[18] = .{
        .ml_name = "green_poll_multi",
        .ml_meth = @ptrCast(&hub.pyGreenPollMulti),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Wait for any of N fds to become ready, with timeout. Returns list of (fd, revents).",
    };
    methods[19] = .{
        .ml_name = "green_poll_fd_timeout",
        .ml_meth = @ptrCast(&hub.pyGreenPollFdTimeout),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Wait for fd readiness with timeout via POLL_ADD + LINK_TIMEOUT. Returns revents or 0 on timeout.",
    };
    methods[20] = .{
        .ml_name = "http_read_request",
        .ml_meth = @ptrCast(&hub.pyHttpReadRequest),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Read and parse one HTTP request from fd. Returns Request object or None on EOF.",
    };
    methods[21] = .{
        .ml_name = "http_send_response",
        .ml_meth = @ptrCast(&hub.pyHttpSendResponse),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Format headers and send response via writev. Zero-copy for body.",
    };
    methods[22] = .{
        .ml_name = "get_active_waits",
        .ml_meth = @ptrCast(&hub.pyGetActiveWaits),
        .ml_flags = py.py_helper_meth_noargs(),
        .ml_doc = "Return hub.active_waits (for testing).",
    };
    methods[23] = .{
        .ml_name = "green_forget_fd",
        .ml_meth = @ptrCast(&hub.pyGreenForgetFd),
        .ml_flags = py.py_helper_meth_varargs(),
        .ml_doc = "Best-effort cleanup for a registered fd closed off the hub thread.",
    };
    methods[24] = py.py_helper_method_sentinel();

    module_def = .{
        .m_base = py.py_helper_module_def_head_init(),
        .m_name = "_framework_core",
        .m_doc = "Low-level Zig extension for the-framework.",
        .m_size = -1,
        .m_methods = &methods,
        .m_slots = null,
        .m_traverse = null,
        .m_clear = null,
        .m_free = null,
    };
    const module = py.py_helper_module_create(&module_def) orelse return null;

    // Intern method strings (GET, POST, etc.)
    if (!lazy_request.initInternedMethods()) return null;

    // Initialize and register LazyRequest type
    lazy_request.initLazyRequestType();
    const request_type: *py.PyTypeObject = @ptrCast(&lazy_request.LazyRequestType);
    if (py.PyType_Ready(request_type) < 0) return null;
    py.py_helper_incref(@ptrCast(request_type));
    if (py.PyModule_AddObject(module, "Request", @ptrCast(request_type)) < 0) return null;

    return module;
}
