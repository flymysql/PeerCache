#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

#ifdef PEERCACHE_NO_RDMA

// Stub module: built when libibverbs/librdmacm are unavailable. Importing works
// (so callers can probe with HAS_RDMA), but constructing the engine raises so the
// pure-Python TCP fallback transport is selected instead.
PYBIND11_MODULE(_peercache, m) {
  m.doc() = "PeerCache C++ data plane (stub build: no RDMA)";
  m.attr("HAS_RDMA") = false;
  m.def("_unavailable", []() {
    throw std::runtime_error(
        "peercache: built without RDMA support (PEERCACHE_NO_RDMA). "
        "Use the TCP fallback transport, or rebuild on a host with "
        "libibverbs/librdmacm.");
  });
}

#else

#include "peercache/transfer_engine.h"
#include "peercache/types.h"

using peercache::MrHandle;
using peercache::ReadRequest;
using peercache::TransferEngine;

PYBIND11_MODULE(_peercache, m) {
  m.doc() = "PeerCache C++ data plane (raw libibverbs one-sided RDMA)";
  m.attr("HAS_RDMA") = true;

  py::class_<MrHandle>(m, "MrHandle")
      .def_readonly("addr", &MrHandle::addr)
      .def_readonly("length", &MrHandle::length)
      .def_readonly("lkey", &MrHandle::lkey)
      .def_readonly("rkey", &MrHandle::rkey);

  py::class_<ReadRequest>(m, "ReadRequest")
      .def(py::init<>())
      .def(py::init([](const std::string& node, uint64_t local_addr,
                       uint64_t remote_addr, uint32_t rkey, uint64_t length) {
             ReadRequest r;
             r.remote_node = node;
             r.local_addr = local_addr;
             r.remote_addr = remote_addr;
             r.rkey = rkey;
             r.length = length;
             return r;
           }),
           py::arg("remote_node"), py::arg("local_addr"), py::arg("remote_addr"),
           py::arg("rkey"), py::arg("length"))
      .def_readwrite("remote_node", &ReadRequest::remote_node)
      .def_readwrite("local_addr", &ReadRequest::local_addr)
      .def_readwrite("remote_addr", &ReadRequest::remote_addr)
      .def_readwrite("rkey", &ReadRequest::rkey)
      .def_readwrite("length", &ReadRequest::length);

  py::class_<TransferEngine>(m, "TransferEngine")
      .def(py::init<const std::vector<std::string>&, uint8_t, int,
                    const std::string&, uint16_t, size_t>(),
           py::arg("device_names"), py::arg("ib_port") = 1,
           py::arg("gid_index") = 3, py::arg("bind_host") = "0.0.0.0",
           py::arg("bind_port") = 0, py::arg("max_channels_per_peer") = 16)
      .def("n_rails", &TransferEngine::n_rails)
      .def("register_mr", &TransferEngine::register_mr, py::arg("addr"),
           py::arg("length"))
      .def("deregister_mr", &TransferEngine::deregister_mr, py::arg("addr"))
      .def("batch_read", &TransferEngine::batch_read, py::arg("requests"),
           py::call_guard<py::gil_scoped_release>())
      .def("batch_read_v", &TransferEngine::batch_read_v,
           py::arg("remote_nodes"), py::arg("local_addrs"),
           py::arg("remote_addrs"), py::arg("rkeys"), py::arg("lengths"),
           py::call_guard<py::gil_scoped_release>())
      .def("batch_read_multi", &TransferEngine::batch_read_multi,
           py::arg("node_ids"), py::arg("local_addrs"), py::arg("remote_addrs"),
           py::arg("lengths"), py::arg("rail_endpoints"), py::arg("rail_rkeys"),
           py::call_guard<py::gil_scoped_release>())
      .def("local_endpoint", &TransferEngine::local_endpoint)
      .def("local_endpoints", &TransferEngine::local_endpoints)
      .def("stats", &TransferEngine::stats);
}

#endif
