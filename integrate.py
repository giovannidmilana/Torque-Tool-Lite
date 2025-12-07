import socket
import threading
import queue
import time
from dataclasses import dataclass
from typing import Optional, Tuple, Iterable, NamedTuple

@dataclass
class ConnectionResponse:
    cell_id: str
    channel_id: str
    controller_name: str
    supplier_code: str
    power_macs_version: str
    controller_software_version: str
    protocol_version: str
    rbu_type: str
    controller_serial_number: Optional[str] = None

# Torque tool configuration
TORQUE_TOOL_IP = "You.row.nip"
TORQUE_TOOL_PORT = 4545

def format_message(mid, revision="001", payload=""):
    payload_bytes = payload.encode('ascii') if payload else b''
    # include trailing NUL in length if you send it (adjust if your device expects otherwise)
    length = 20 + len(payload_bytes) + 1
    mid_int = int(mid) if isinstance(mid, str) and mid.isdigit() else mid
    header = (
        f"{length:04}"
        f"{mid_int:04}"
        f"{revision}"
        f" "
        f" "
        f" "
        f"      "
    ).encode('ascii')
    message = header + payload_bytes + b'\x00'
    print(f"-> SEND (hex): {message.hex()}")
    return message

class SocketReceiver(threading.Thread):
    """
    Read from socket, extract messages framed by 4-byte ASCII length header.
    This version tolerates leading NULs or small amounts of garbage and will
    attempt to re-sync instead of dying immediately on a parse error.
    """
    def __init__(self, sock: socket.socket, out_q: queue.Queue, recv_size: int = 4096):
        super().__init__(daemon=True)
        self.sock = sock
        self.out_q = out_q
        self.recv_size = recv_size
        self._buf = bytearray()
        self._stop = threading.Event()

    def run(self):
        try:
            while not self._stop.is_set():
                try:
                    data = self.sock.recv(self.recv_size)
                except socket.timeout:
                    continue
                except socket.error as e:
                    print(f"Receiver socket error: {e}")
                    self.out_q.put(None)
                    break

                if not data:
                    # connection closed by peer
                    print("Receiver: peer closed connection")
                    self.out_q.put(None)
                    break

                print(f"<- RECV chunk (hex, len={len(data)}): {data.hex()}")
                self._buf.extend(data)

                # Keep extracting full messages
                while True:
                    # Skip leading NULs (0x00) which some devices use as separators
                    while self._buf and self._buf[0] == 0x00:
                        del self._buf[0]

                    if len(self._buf) < 4:
                        break

                    # Try parse length; if it fails, try to find next location where 4 ascii digits exist
                    try:
                        length = int(self._buf[0:4].decode('ascii'))
                    except Exception:
                        # try to find resync point where 4 consecutive ASCII digits appear
                        found = False
                        max_search_start = max(0, len(self._buf) - 4)
                        for i in range(1, max_search_start + 1):
                            slice4 = self._buf[i:i+4]
                            if len(slice4) == 4 and all(48 <= b <= 57 for b in slice4):
                                # drop bytes before that position and try again next loop
                                print(f"Resync: dropping {i} bytes from buffer to find header")
                                del self._buf[:i]
                                found = True
                                break
                        if not found:
                            # Not enough to resync yet; wait for more data
                            break
                        else:
                            continue  # go back and parse length at new buffer start

                    # If we have length but not whole message, wait for more data
                    if length <= 0:
                        print(f"Invalid length={length}, discarding one byte and resyncing.")
                        del self._buf[0]
                        continue
                    if len(self._buf) < length:
                        # incomplete message
                        break

                    # Extract and emit a full message
                    msg = bytes(self._buf[:length])
                    del self._buf[:length]
                    self.out_q.put(msg)
        finally:
            # signal shutdown to consumer
            self.out_q.put(None)

    def stop(self):
        self._stop.set()

def connect_to_torque_tool(timeout=2.0):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.connect((TORQUE_TOOL_IP, TORQUE_TOOL_PORT))
        print("Connected to the torque tool.")
        return sock
    except socket.error as e:
        print(f"Socket error on connect: {e}")
    return None

def parse_mid_0002(response: bytes) -> Tuple[bool, Optional[ConnectionResponse]]:
    try:
        response_str = response.decode('ascii', errors='ignore').rstrip('\x00')
        if response_str[4:8] != "0002":
            return False, None
        revision = response_str[8:11]
        if revision == "001":
            cell_id = response_str[20:24].strip()
            channel_id = response_str[24:28].strip()
            controller_name = response_str[28:48].strip()
            supplier_code = response_str[48:52].strip()
            power_macs_version = response_str[52:56].strip()
            controller_software_version = response_str[56:64].strip()
            return True, ConnectionResponse(
                cell_id=cell_id, channel_id=channel_id, controller_name=controller_name,
                supplier_code=supplier_code, power_macs_version=power_macs_version,
                controller_software_version=controller_software_version,
                protocol_version="1.0", rbu_type="Unknown"
            )
        elif revision == "003":
            cell_id = response_str[20:24].strip()
            channel_id = response_str[24:28].strip()
            controller_name = response_str[28:48].strip()
            supplier_code = response_str[48:52].strip()
            power_macs_version = response_str[52:56].strip()
            controller_software_version = response_str[56:64].strip()
            rbu_type = response_str[64:68].strip()
            controller_serial_number = response_str[68:88].strip()
            return True, ConnectionResponse(
                cell_id=cell_id, channel_id=channel_id, controller_name=controller_name,
                supplier_code=supplier_code, power_macs_version=power_macs_version,
                controller_software_version=controller_software_version,
                protocol_version="3.0", rbu_type=rbu_type, controller_serial_number=controller_serial_number
            )
        else:
            print(f"Unsupported revision: {revision}")
            return False, None
    except Exception as e:
        print(f"Error parsing MID 0002: {e}")
        return False, None

def parse_mid_0061(response: bytes):
    try:
        response_str = response.decode('ascii', errors='ignore')
        length = int(response_str[0:4])
        mid = response_str[4:8]
        revision = response_str[8:11]
        if mid != "0061":
            print("parse_mid_0061 called with wrong MID:", mid)
            return None
        print(f"Parsing MID 0061: length={length}, revision={revision}")
        batch_size = response_str[106:110].strip() if len(response_str) > 110 else ''
        batch_counter = response_str[114:118].strip() if len(response_str) > 118 else ''
        tightening_status = response_str[120] if len(response_str) > 120 else ''
        batch_status = response_str[123] if len(response_str) > 123 else ''
        torque = response_str[183:189].strip() if len(response_str) > 189 else '0'
        angle = response_str[212:217].strip() if len(response_str) > 217 else '0'
        tightening_id = response_str[303:313].strip() if len(response_str) > 313 else ''
        timestamp = response_str[345:364].strip() if len(response_str) > 364 else ''
        tightening_ok = tightening_status == '1'
        batch_ok = batch_status == '1'
        torque_value = int(torque) / 100 if torque.isdigit() else 0.0
        angle_value = int(angle) if angle.isdigit() else 0
        print("Parsed MID 0061 Data:")
        print(f" Batch Size: {batch_size}")
        print(f" Batch Counter: {batch_counter}")
        print(f" Tightening OK: {tightening_ok}")
        print(f" Batch OK: {batch_ok}")
        print(f" Torque: {torque_value} Nm")
        print(f" Angle: {angle_value} degrees")
        print(f" Tightening ID: {tightening_id}")
        print(f" Timestamp: {timestamp}")
        return {
            "batch_size": batch_size,
            "batch_counter": batch_counter,
            "tightening_ok": tightening_ok,
            "batch_ok": batch_ok,
            "torque_value": torque_value,
            "angle_value": angle_value,
            "tightening_id": tightening_id,
            "timestamp": timestamp,
        }
    except Exception as e:
        print(f"Error parsing MID 0061: {e}")
        return None

def send_keep_alive_loop(sock: socket.socket, stop_event: threading.Event, interval: float = 10.0):
    while not stop_event.is_set():
        msg = format_message("9999", "001")
        try:
            sock.sendall(msg)
            print("Sent keep-alive (MID 9999).")
        except socket.error as e:
            print("Keep-alive send failed:", e)
            break
        stop_event.wait(interval)

def send_connection_request(sock: socket.socket):
    msg = format_message("0001", "003")
    sock.sendall(msg)
    print("Sent connection request (0001).")

def send_LTR_subscription_request(sock: socket.socket):
    msg = format_message("0060", "003")
    sock.sendall(msg)
    print("Sent LTR subscription (0060).")


def send_select_job(sock: socket.socket, job_id: int, revision: str = "002") -> None:
    """
    MID 0038 - Select Job

    Selects a job on the controller. This does NOT start the job; it just makes it active.
    - revision '001': 2-digit job ID (00-99)
    - revision '002': 4-digit job ID (0000-9999)
    """
    if revision == "001":
        job_id_str = f"{job_id:02d}"
    else:
        # Default to revision 2 (4 ASCII digits)
        revision = "002"
        job_id_str = f"{job_id:04d}"

    msg = format_message("0038", revision, payload=job_id_str)
    sock.sendall(msg)
    print(f"Sent select job (0038), job_id={job_id_str}, rev={revision}.")

def send_job_restart(sock: socket.socket, job_id: int, revision: str = "002") -> None:
    """
    MID 0039 - Job restart

    Requests a restart of the given job.
    - revision '001': 2-digit job ID (00-99)
    - revision '002': 4-digit job ID (0000-9999)
    """
    if revision == "001":
        job_id_str = f"{job_id:02d}"
    else:
        revision = "002"
        job_id_str = f"{job_id:04d}"

    msg = format_message("0039", revision, payload=job_id_str)
    sock.sendall(msg)
    print(f"Sent job restart (0039), job_id={job_id_str}, rev={revision}.")

def send_job_abort(sock: socket.socket, revision: str = "001") -> None:
    """
    MID 0127 - Abort Job

    Aborts the current running job, if there is one.
    This MID has no payload in the basic form.
    """
    msg = format_message("0127", revision)
    sock.sendall(msg)
    print(f"Sent job abort (0127), rev={revision}.")


class DynamicJobProgram(NamedTuple):
    """
    Represents one entry in the dynamic Job list.

    channel_id: 0-99
    program_id: 0-999
    auto_select: bool (True = 1, False = 0)
    batch_size: 0-99
    max_coherent_nok: 0-99
    """
    channel_id: int
    program_id: int
    auto_select: bool
    batch_size: int
    max_coherent_nok: int


def send_dynamic_job(
    sock: socket.socket,
    job_id: int,
    job_name: str,
    programs: Iterable[DynamicJobProgram],
    revision: str = "001",
) -> None:
    """
    MID 0140 - Execute dynamic Job request (basic revision 1).

    This builds a minimal dynamic Job payload including:
      - 01: Job ID (4 digits)
      - 02: Job name (25 chars, padded/truncated)
      - 03: Number of parameter sets (2 digits)
      - 04: Job list (N * 15-byte entries)

    Note: Additional parameters (05..19) from the spec are NOT included here and
    will use controller defaults. Extend this function if you need more control.
    """

    # ---- 01: Job ID ----
    job_id_str = f"{job_id:04d}"
    payload_parts = ["01", job_id_str]

    # ---- 02: Job name ----
    job_name_fixed = job_name[:25].ljust(25)
    payload_parts.extend(["02", job_name_fixed])

    # ---- 03: Number of parameter sets ----
    programs_list = list(programs)
    num_programs = len(programs_list)
    if num_programs > 99:
        raise ValueError("Dynamic Job supports at most 99 programs.")
    num_programs_str = f"{num_programs:02d}"
    payload_parts.extend(["03", num_programs_str])

    # ---- 04: Job list ----
    # Each entry: [Channel-ID]:[Program-ID]:[AutoSelect]:[BatchSize]:[MaxCoherentNOK];
    job_list_entries = []
    for p in programs_list:
        ch = f"{p.channel_id:02d}"
        prog = f"{p.program_id:03d}"
        auto = "1" if p.auto_select else "0"
        batch_size = f"{p.batch_size:02d}"
        max_nok = f"{p.max_coherent_nok:02d}"
        entry = f"{ch}:{prog}:{auto}:{batch_size}:{max_nok};"
        job_list_entries.append(entry)

    job_list_str = "".join(job_list_entries)
    payload_parts.extend(["04", job_list_str])

    payload = "".join(payload_parts)

    msg = format_message("0140", revision, payload=payload)
    sock.sendall(msg)
    print(
        f"Sent dynamic job (0140), job_id={job_id_str}, "
        f"programs={num_programs}, rev={revision}."
    )

programs = [
    DynamicJobProgram(channel_id=1, program_id=45, auto_select=False, batch_size=10, max_coherent_nok=2),
    DynamicJobProgram(channel_id=1, program_id=46, auto_select=True, batch_size=5, max_coherent_nok=1),
]

# Not used in loops yet, but ready:
# send_dynamic_job(sock, job_id=1001, job_name="Example Dynamic Job", programs=programs)

def send_old_tightening_request(
    sock: socket.socket,
    tightening_id: int,
    revision: str = "001",
) -> None:
    """
    MID 0064 - Old tightening result upload request.

    Requests upload of a particular tightening result identified by its unique tightening ID.
    - tightening_id 0 means "latest tightening result".
    - The ID is sent as 10 ASCII digits (zero-padded).

    The response from the controller is MID 0065 (old tightening result upload reply).
    """
    if tightening_id < 0 or tightening_id > 4294967295:
        raise ValueError("tightening_id must be between 0 and 4294967295.")

    tightening_id_str = f"{tightening_id:010d}"  # 10 ASCII digits
    msg = format_message("0064", revision, payload=tightening_id_str)
    sock.sendall(msg)
    print(f"Sent old tightening request (0064), id={tightening_id_str}, rev={revision}.")



def main():
    print("Starting connection to torque tool...")
    sock = connect_to_torque_tool()
    if not sock:
        print("Failed to connect.")
        return

    msg_q = queue.Queue()
    receiver = SocketReceiver(sock, msg_q, recv_size=4096)
    receiver.start()

    stop_keepalive = threading.Event()
    ka_thread = threading.Thread(target=send_keep_alive_loop, args=(sock, stop_keepalive, 10.0), daemon=True)
    ka_thread.start()

    try:
        send_connection_request(sock)
        time.sleep(0.2)
        send_LTR_subscription_request(sock)
        time.sleep(0.2)

        while True:
            msg = msg_q.get()
            if msg is None:
                print("Connection closed or receiver stopped.")
                break
            try:
                header = msg[:20].decode('ascii', errors='replace')
                print(f"Message header: {header}")
            except Exception:
                pass
            mid = msg[4:8].decode('ascii', errors='ignore')
            if mid == "0002":
                ok, conn = parse_mid_0002(msg)
                if ok:
                    print("Connected:", conn.controller_name)
            elif mid == "0061":
                parse_mid_0061(msg)
            elif mid == "9999":
                print("Keep-alive acknowledged (9999).")
            else:
                print("Unhandled MID:", mid)
    finally:
        print("Shutting down.")
        stop_keepalive.set()
        receiver.stop()
        sock.close()

if __name__ == "__main__":
    main()
