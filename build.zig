const std = @import("std");

fn detectPython(b: *std.Build, script: []const u8) []const u8 {
    const stdout = b.run(&.{ "python3", "-c", script });
    return std.mem.trim(u8, stdout, &std.ascii.whitespace);
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Auto-detect Python paths, with user overrides
    const python_include = b.option([]const u8, "python-include", "Python include directory (e.g. /usr/include/python3.14)") orelse detectPython(b, "import sysconfig; print(sysconfig.get_path('include'))");
    const greenlet_include = b.option([]const u8, "greenlet-include", "Greenlet include directory (contains greenlet.h)") orelse detectPython(b, "import greenlet; import os; print(os.path.dirname(greenlet.__file__))");
    const python_lib = b.option([]const u8, "python-lib", "Python library directory (e.g. /usr/lib)") orelse detectPython(b, "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))");
    const ext_suffix = b.option([]const u8, "ext-suffix", "Python extension suffix (e.g. .cpython-314-x86_64-linux-gnu.so)") orelse detectPython(b, "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX').removesuffix('.so'))");

    // -----------------------------------------------------------------------
    // Shared library: libframework.so
    // -----------------------------------------------------------------------
    const lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "framework",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });

    b.installArtifact(lib);

    // -----------------------------------------------------------------------
    // Python C extension: _framework_core.<ext_suffix>.so
    // -----------------------------------------------------------------------
    const ext_name = b.fmt("_framework_core{s}", .{ext_suffix});

    const ext_module = b.createModule(.{
        .root_source_file = b.path("src/extension.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    ext_module.addIncludePath(b.path("src"));
    ext_module.addIncludePath(.{ .cwd_relative = python_include });
    ext_module.addIncludePath(.{ .cwd_relative = greenlet_include });

    ext_module.addCSourceFile(.{
        .file = b.path("src/py_helpers.c"),
        .flags = &.{},
    });

    const ext = b.addLibrary(.{
        .linkage = .dynamic,
        .name = ext_name,
        .root_module = ext_module,
    });

    // Install with the correct name (no "lib" prefix) so Python can import it
    const ext_install = b.addInstallFile(ext.getEmittedBin(), b.fmt("lib/{s}.so", .{ext_name}));
    b.getInstallStep().dependOn(&ext_install.step);

    // -----------------------------------------------------------------------
    // hparse dependency (HTTP parser)
    // -----------------------------------------------------------------------
    const hparse_dep = b.dependency("hparse", .{ .target = target, .optimize = optimize });
    const hparse_mod = hparse_dep.module("hparse");

    // Wire hparse into the extension module so hub.zig can import http.zig
    ext_module.addImport("hparse", hparse_mod);

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    // Tests for main.zig (existing)
    const lib_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    // Tests for ring.zig
    const ring_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/ring.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ring_tests = b.addRunArtifact(ring_tests);

    // Tests for connection.zig (needs link_libc for arena's c_allocator)
    const conn_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/connection.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    const run_conn_tests = b.addRunArtifact(conn_tests);

    // Tests for http.zig (HTTP request parser using hparse)
    const http_test_mod = b.createModule(.{
        .root_source_file = b.path("src/http.zig"),
        .target = target,
        .optimize = optimize,
    });
    http_test_mod.addImport("hparse", hparse_mod);
    const http_tests = b.addTest(.{ .root_module = http_test_mod });
    const run_http_tests = b.addRunArtifact(http_tests);

    // Tests for http_response.zig (HTTP response writer)
    const http_resp_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/http_response.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_http_resp_tests = b.addRunArtifact(http_resp_tests);

    // Tests for op_slot.zig (operation slot table)
    const op_slot_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/op_slot.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_op_slot_tests = b.addRunArtifact(op_slot_tests);

    // Tests for config.zig (arena config)
    const config_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/config.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_config_tests = b.addRunArtifact(config_tests);

    // Tests for chunk_pool.zig (chunk recycler)
    const chunk_pool_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/chunk_pool.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_chunk_pool_tests = b.addRunArtifact(chunk_pool_tests);

    // Tests for arena.zig (per-connection request arena)
    const arena_test_mod = b.createModule(.{
        .root_source_file = b.path("src/arena.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    const arena_tests = b.addTest(.{ .root_module = arena_test_mod });
    const run_arena_tests = b.addRunArtifact(arena_tests);

    // Tests for buffer_recycler.zig (power-of-2 buffer recycling)
    const buf_recycler_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/buffer_recycler.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_buf_recycler_tests = b.addRunArtifact(buf_recycler_tests);

    // Tests for input_buffer.zig (contiguous recv buffer)
    const input_buffer_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/input_buffer.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_input_buffer_tests = b.addRunArtifact(input_buffer_tests);

    // Tests for test_input_buffer.zig (comprehensive InputBuffer + BufferRecycler tests)
    const input_buffer_integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/test_input_buffer.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_input_buffer_integration_tests = b.addRunArtifact(input_buffer_integration_tests);

    // Tests for hub.zig (tryParse, etc. — needs Python/greenlet headers and hparse)
    const hub_test_mod = b.createModule(.{
        .root_source_file = b.path("src/hub.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    hub_test_mod.addImport("hparse", hparse_mod);
    hub_test_mod.addIncludePath(b.path("src"));
    hub_test_mod.addIncludePath(.{ .cwd_relative = python_include });
    hub_test_mod.addIncludePath(.{ .cwd_relative = greenlet_include });
    hub_test_mod.addLibraryPath(.{ .cwd_relative = python_lib });
    hub_test_mod.linkSystemLibrary("python3.14", .{});
    hub_test_mod.addCSourceFile(.{
        .file = b.path("src/py_helpers.c"),
        .flags = &.{},
    });
    const hub_tests = b.addTest(.{ .root_module = hub_test_mod });
    const run_hub_tests = b.addRunArtifact(hub_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_ring_tests.step);
    test_step.dependOn(&run_conn_tests.step);
    test_step.dependOn(&run_http_tests.step);
    test_step.dependOn(&run_http_resp_tests.step);
    test_step.dependOn(&run_op_slot_tests.step);
    test_step.dependOn(&run_config_tests.step);
    test_step.dependOn(&run_chunk_pool_tests.step);
    test_step.dependOn(&run_arena_tests.step);
    test_step.dependOn(&run_buf_recycler_tests.step);
    test_step.dependOn(&run_input_buffer_tests.step);
    test_step.dependOn(&run_input_buffer_integration_tests.step);
    test_step.dependOn(&run_hub_tests.step);
}
