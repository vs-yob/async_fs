// async_fs.cpp – eventfd‑driven, non‑blocking I/O using io_uring
// build: c++ -std=c++17 -fPIC -shared -I$(python3 -m pybind11 --includes) async_fs.cpp -luring -o async_fs$(python3-config --extension-suffix)

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <fcntl.h>
#include <unistd.h>
#include <liburing.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <cstring>
#include <cstdint>
#include <memory>
#include <cerrno>

/*--------------------------------------------------------------------------
  Global context – one io_uring + eventfd shared by all requests
  --------------------------------------------------------------------------*/

namespace {
constexpr unsigned RING_ENTRIES = 256;

struct Context {
    io_uring ring{};
    int efd{-1};          // eventfd for completions
    PyObject* loop{nullptr};   // borrowed ref to running loop
    PyObject* reader_cb{nullptr}; // callable that processes CQEs
} ctx;

/*--------------------------------------------------------------------------
  Small RAII helpers
  --------------------------------------------------------------------------*/
struct FdGuard {
    int fd;
    explicit FdGuard(int fd = -1) : fd(fd) {}
    ~FdGuard() { if (fd != -1) ::close(fd); }
    FdGuard(const FdGuard&) = delete;
    FdGuard& operator=(const FdGuard&) = delete;
    operator bool() const { return fd != -1; }
};

struct BufferGuard {
    std::unique_ptr<char[]> buf;
    explicit BufferGuard(size_t n) : buf(new(std::nothrow) char[n]) {}
    char* get() { return buf.get(); }
};

/*--------------------------------------------------------------------------
  Request object stored in sqe->user_data
  --------------------------------------------------------------------------*/
struct Request {
    PyObject* future; // owned ref
    BufferGuard* buffer; // may be nullptr (for write / metadata ops)
    bool is_read;
    long long length;   // bytes read/written
};

static void finalize_request(Request* req, long long result) {
    /* Acquire GIL – we are called from Python add_reader → already have it */
    if (result >= 0) {
        PyObject* py_res = nullptr;
        if (req->is_read) {
            py_res = PyBytes_FromStringAndSize(req->buffer->get(), result);
        } else {
            py_res = PyLong_FromLongLong(result);
        }
        if (py_res) {
            PyObject_CallMethod(req->future, "set_result", "O", py_res);
            Py_DECREF(py_res);
        }
    } else {
        PyObject* err = PyErr_SetFromErrno(PyExc_OSError);
        PyErr_Clear(); // Convert to normal object
        PyObject_CallMethod(req->future, "set_exception", "O", err);
        Py_DECREF(err);
    }
    Py_DECREF(req->future);
    delete req->buffer;
    delete req;
}

/*--------------------------------------------------------------------------
  CQE processing callback.  Registered via loop.add_reader(ctx.efd,…)
  --------------------------------------------------------------------------*/
static PyObject* process_completions(PyObject*, PyObject*) {
    /* Read and clear eventfd counter */
    uint64_t dummy;
    ::read(ctx.efd, &dummy, sizeof(dummy));

    struct io_uring_cqe* cqe{};
    while (io_uring_peek_cqe(&ctx.ring, &cqe) == 0) {
        auto* req = reinterpret_cast<Request*>(io_uring_cqe_get_data(cqe));
        long long res = cqe->res;
        io_uring_cqe_seen(&ctx.ring, cqe);
        finalize_request(req, res);
    }
    Py_RETURN_NONE;
}

/*--------------------------------------------------------------------------
  Helper to build/return asyncio.Future, enqueue SQE, submit
  --------------------------------------------------------------------------*/
static PyObject* submit_read(const char* path, Py_ssize_t buf_size) {
    FdGuard fd(::open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (!fd)
        return PyErr_SetFromErrnoWithFilename(PyExc_OSError, path);

    BufferGuard* buf = new BufferGuard(buf_size);
    if (!buf->buf)
        return PyErr_NoMemory();

    // Create Future on captured event‑loop
    PyObject* future = PyObject_CallMethod(ctx.loop, "create_future", nullptr);
    if (!future) { delete buf; return nullptr; }

    auto* req = new Request{future, buf, true, 0};

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.ring);
    io_uring_prep_read(sqe, fd.fd, buf->get(), buf_size, 0);
    io_uring_sqe_set_data(sqe, req);

    Py_BEGIN_ALLOW_THREADS
    io_uring_submit(&ctx.ring);
    Py_END_ALLOW_THREADS

    // fd_guard will close when out of scope (immediately after submission)
    return future; // borrowed by Python caller
}

static PyObject* submit_write(const char* path, const char* data, Py_ssize_t len) {
    FdGuard fd(::open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0644));
    if (!fd)
        return PyErr_SetFromErrnoWithFilename(PyExc_OSError, path);

    // No need to copy data – but we should keep buffer alive until completion
    BufferGuard* buf = new BufferGuard(len);
    if (!buf->buf)
        return PyErr_NoMemory();
    std::memcpy(buf->get(), data, len);

    PyObject* future = PyObject_CallMethod(ctx.loop, "create_future", nullptr);
    if (!future) { delete buf; return nullptr; }

    auto* req = new Request{future, buf, false, len};

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.ring);
    io_uring_prep_write(sqe, fd.fd, buf->get(), len, 0);
    io_uring_sqe_set_data(sqe, req);

    Py_BEGIN_ALLOW_THREADS
    io_uring_submit(&ctx.ring);
    Py_END_ALLOW_THREADS

    return future;
}

/*--------------------------------------------------------------------------
  Python‑visible wrappers
  --------------------------------------------------------------------------*/
static PyObject* py_read_file(PyObject*, PyObject* args) {
    const char* path; Py_ssize_t size;
    if (!PyArg_ParseTuple(args, "sn", &path, &size)) return nullptr;
    return submit_read(path, size);
}

static PyObject* py_write_file(PyObject*, PyObject* args) {
    const char* path; const char* data; Py_ssize_t len;
    if (!PyArg_ParseTuple(args, "ss#", &path, &data, &len)) return nullptr;
    return submit_write(path, data, len);
}

static PyObject* py_delete_file(PyObject*, PyObject* args) {
    const char* path; if (!PyArg_ParseTuple(args, "s", &path)) return nullptr;
    if (::unlink(path) == -1) return PyErr_SetFromErrnoWithFilename(PyExc_OSError, path);
    Py_RETURN_NONE; // fast op, synchronous
}

static PyObject* py_move_file(PyObject*, PyObject* args) {
    const char* src; const char* dst;
    if (!PyArg_ParseTuple(args, "ss", &src, &dst)) return nullptr;
    if (::rename(src, dst) == -1) return PyErr_SetFromErrno(PyExc_OSError);
    Py_RETURN_NONE;
}

static PyObject* py_file_info(PyObject*, PyObject* args) {
    const char* path; if (!PyArg_ParseTuple(args, "s", &path)) return nullptr;
    struct stat st; if (::stat(path, &st) == -1)
        return PyErr_SetFromErrnoWithFilename(PyExc_OSError, path);
    return Py_BuildValue("{sK,sK,sK,sK}",
                         "size", static_cast<unsigned long long>(st.st_size),
                         "mode", static_cast<unsigned long long>(st.st_mode),
                         "mtime", static_cast<unsigned long long>(st.st_mtime),
                         "atime", static_cast<unsigned long long>(st.st_atime));
}

/*--------------------------------------------------------------------------
  Module definition tables
  --------------------------------------------------------------------------*/
static PyMethodDef methods[] = {
    {"read_file",  py_read_file,  METH_VARARGS, "Asynchronously read file"},
    {"write_file", py_write_file, METH_VARARGS, "Asynchronously write file"},
    {"delete_file",py_delete_file,METH_VARARGS, "Delete file (sync)"},
    {"move_file",  py_move_file,  METH_VARARGS, "Move/Rename file (sync)"},
    {"file_info",  py_file_info,  METH_VARARGS, "Stat file (sync)"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "async_fs",
    "Fast non‑blocking file I/O with io_uring",
    -1,
    methods
};

/*--------------------------------------------------------------------------
  Module init – set up ring + eventfd and hook add_reader
  --------------------------------------------------------------------------*/
PyMODINIT_FUNC PyInit_async_fs(void) {
    if (io_uring_queue_init(RING_ENTRIES, &ctx.ring, 0) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "io_uring_queue_init failed");
        return nullptr;
    }

    ctx.efd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (ctx.efd == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        return nullptr;
    }
    if (io_uring_register_eventfd(&ctx.ring, ctx.efd) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "io_uring_register_eventfd failed");
        return nullptr;
    }

    /* Grab current event‑loop */
    PyObject* asyncio = PyImport_ImportModule("asyncio");
    if (!asyncio) return nullptr;
    ctx.loop = PyObject_CallMethod(asyncio, "get_event_loop", nullptr);
    Py_DECREF(asyncio);
    if (!ctx.loop) return nullptr;

    /* Register Python callback */
    ctx.reader_cb = PyCFunction_NewEx(& (PyMethodDef){"_proc", process_completions, METH_NOARGS, nullptr}, nullptr, nullptr);
    if (!ctx.reader_cb) return nullptr;

    PyObject* add_reader_res = PyObject_CallMethod(ctx.loop, "add_reader", "iO", ctx.efd, ctx.reader_cb);
    if (!add_reader_res) return nullptr;
    Py_DECREF(add_reader_res);

    return PyModule_Create(&module);
}

} // anonymous namespace
