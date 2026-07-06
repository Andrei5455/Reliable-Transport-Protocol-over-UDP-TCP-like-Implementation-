# 🚀 Reliable Transport Protocol over UDP (TCP-like Implementation)

---

## 📌 Project Overview
This project implements a **reliable, connection-oriented transport protocol over an unreliable UDP layer**, mimicking core TCP functionalities from scratch in C. The architecture is built on a **dual-threaded model** for both client and server entities to decouple application I/O from asynchronous network packet handling.

---

## ⚙️ Architecture & Threading Model
Each communication node (Client/Server) operates asynchronously using two dedicated threads:
* **Application Thread:** Handles the upper-layer interface, actively executing calls to `send_data()` and `recv_data()`.
* **Background Handler Thread (`sender_handler` / `receiver_handler`):** Strictly manages low-level packet transmissions, selective acknowledgments (ACKs), timer expirations, and window management without blocking the application thread.

---

## 🛠️ Core Features & Technical Implementation

### 1. Connection Establishment (Three-Way Handshake)
Connection setup is achieved via a robust **3-Way Handshake** (`SYN` -> `SYN-ACK` -> `ACK`).
* **Concurrency Handling:** To support multiple concurrent client connection attempts without packet collision, the server utilizes a thread-safe, static queue (`syn_queue_pkts`) protected by a mutex. While the server processes an active handshake, subsequent incoming `SYN` requests are safely buffered, preventing data race conditions and memory overwrites.

---

### 2. Reliable Data Transfer (Selective Repeat)
Reliability is enforced through a **Selective Repeat Sliding Window** algorithm:
* **Out-of-Order Buffering:** Sent packets are buffered within a transmission window (`window_buffers`). When a packet is lost in transit, the receiver caches out-of-order deliveries instead of dropping them. The sender selectively retransmits only the missing sequence number upon timer expiration.
* **Low-Latency Timers:** Timeout detection is implemented using POSIX `timerfd` with a high-resolution interval of **20ms** — optimized to cover the Bandwidth-Delay Product (BDP) inside the Mininet emulation environment without causing network stalling.

---

### 3. Integrated Flow Control & Congestion Control
To maximize throughput while preventing receiver buffer overflow and network congestion, the protocol dynamically computes its effective window size:
* **Congestion Control (BDP Optimization):** During setup, the transmission window limit (`max_window_seq`) is derived from the **Bandwidth-Delay Product (BDP)** divided by the maximum payload size (`MAX_DATA_SIZE`). This dictates the absolute upper bound of packets in-flight.
* **Flow Control:** Every incoming ACK from the receiver includes a `recv_window` field advertising the remaining free space in the application's receive buffer (`MAX_BUFFER_SIZE - buffer_len`).
* **Dynamic Window Adaptation:** On each ACK arrival, the sender evaluates the advertised buffer space and restricts transmission using the equation:
  $$\text{Effective Window} = \min(\text{max\_window\_seq}, \text{recv\_window\_size})$$

---

### 4. Handling 16-Bit Sequence Number Overflow (Large Files)
The protocol header mandates a **16-bit sequence number**, which naturally overflows after 65,535 packets (~33 MB of data). Unhandled, this causes sequence wrap-around, leading the receiver to drop new packets as "duplicates" or "stale".
* **32-Bit Sequence Reconstruction:** Implemented a continuous reconstruction logic using `base16` and `base32_high` trackers. When a severe sequence discontinuity is detected (e.g., expecting packet `65000` but receiving `10`), the receiver deduces a wrap-around event and automatically increments the upper 16-bit high-order base, maintaining absolute ordering for multi-gigabyte file transfers.

---

### 5. Thread Synchronization & Deadlock Prevention
* **Condition Variables (`pthread_cond_t`):** To avoid CPU-intensive busy-waiting, if an application thread attempts to read from an empty buffer (`recv_data`) or write to a full buffer (`send_data`), it is suspended via `pthread_cond_wait`. The background handler immediately wakes the thread via `pthread_cond_broadcast` as soon as buffer space or new payload arrives.
* **Deadlock Mitigation at File Termination:** If a receive timeout occurs (`res == -1`), the background handler forces a flush of any consecutively reordered packets from the internal sliding window directly into the application buffer. This prevents a common EOF deadlock caused by the loss of final trailing ACKs.
