# Beacon eddy current scanner support
#
# Copyright (C) 2020-2023 Matt Baker <baker.matt.j@gmail.com>
# Copyright (C) 2020-2023 Lasse Dalegaard <dalegaard@gmail.com>
# Copyright (C) 2023 Beacon <beacon3d.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license.
#
# --------------------------------------------------------------------
# --- Modified by Javier Miller
# --- This version includes extensive modifications and bug fixes for:
# ---   - Delta printer kinematics (round bed) support
# ---   - Fast "fly-by" mesh scanning ("Empty clusters" error)
# ---   - Startup crash fixes (AlphaBetaFilter & Accelerometer)
# ---   - Kinematic-aware safety checks for delta printers
# ---   - Performance optimizations for sample processing
# --------------------------------------------------------------------
#
import threading
import multiprocessing
import subprocess
import os
import importlib
import traceback
import logging
import chelper
import pins
import math
import time
import queue
import struct
import numpy as np
import copy
import collections
import itertools
from numpy.polynomial import Polynomial
from . import manual_probe
from . import probe
from . import bed_mesh
from . import thermistor
from . import adxl345
from .homing import HomingMove
from mcu import MCU, MCU_trsync
from clocksync import SecondarySync
import msgproto

# --- Module Constants ---

STREAM_BUFFER_LIMIT_DEFAULT = 100
STREAM_TIMEOUT = 1.0
API_DUMP_FIELDS = ["dist", "temp", "pos", "freq", "time"]
TRSYNC_TIMEOUT = 0.025
GRAVITY = 9.80655
ACCEL_BYTES_PER_SAMPLE = 6

Accel_Measurement = collections.namedtuple(
    "Accel_Measurement", ("time", "accel_x", "accel_y", "accel_z")
)


class BeaconProbe:
    """
    Main class for the Beacon sensor.
    Handles communication, calibration, and probing logic.
    """
    # --- Constants ---
    CALIBRATION_POLY_DEGREE = 9
    CALIBRATION_SAMPLE_SYNC_COUNT = 50
    CALIBRATION_DWELL_TIME = 0.250
    
    PROBE_DEFAULT_SKIP_SAMPLES = 5
    PROBE_DEFAULT_SAMPLE_COUNT = 10
    
    CONTACT_PROBE_TARGET_Z = -2.0 # Target Z for contact probe moves
    
    POKE_DEFAULT_TOP = 5.0
    POKE_DEFAULT_BOTTOM = -0.3
    POKE_DEFAULT_SPEED = 3.0
    
    AUTOCAL_DEFAULT_SPEED = 3.0
    AUTOCAL_DEFAULT_ACCEL = 100.0
    AUTOCAL_DEFAULT_RETRACT = 2.0
    AUTOCAL_DEFAULT_RETRACT_SPEED = 10.0
    AUTOCAL_DEFAULT_SAMPLES = 3
    AUTOCAL_DEFAULT_TOLERANCE = 0.008
    AUTOCAL_DEFAULT_MAX_RETRIES = 3
    
    OFFSET_COMPARE_DEFAULT_TOP = 2.0
    
    TEMP_COMP_MIN_VALID = -40.0
    TEMP_COMP_MAX_VALID = 180.0
    
    FIRMWARE_UPDATE_SCRIPT = "update_firmware.py"

    def __init__(self, config, sensor_id):
        self.id = sensor_id
        self.printer = printer = config.get_printer()
        self.reactor = printer.get_reactor()
        self.name = config.get_name()
        self.gcode = printer.lookup_object("gcode")

        # --- ONE-TIME SETUP: Detect Kinematics ---
        # Check for 'print_radius' in [printer] config to detect Delta.
        # We store this in 'self' so all commands can check it safely later.
        printer_config = config.getsection("printer")
        print_radius = printer_config.getfloat("print_radius", None, above=0.0)
        self.is_delta = print_radius is not None
        # -----------------------------------------

        self.speed = config.getfloat("speed", 5.0, above=0.0)
        self.lift_speed = config.getfloat("lift_speed", self.speed, above=0.0)
        self.backlash_comp = config.getfloat("backlash_comp", 0.5)

        self.x_offset = config.getfloat("x_offset", 0.0)
        self.y_offset = config.getfloat("y_offset", 0.0)

        self.trigger_distance = config.getfloat("trigger_distance", 2.0)
        self.trigger_dive_threshold = config.getfloat("trigger_dive_threshold", 1.0)
        self.trigger_hysteresis = config.getfloat("trigger_hysteresis", 0.006)
        self.z_settling_time = config.getint("z_settling_time", 5, minval=0)
        self.default_probe_method = config.getchoice(
            "default_probe_method",
            {"contact": "contact", "proximity": "proximity"},
            "proximity",
        )

        # If using paper for calibration, this would be .1mm
        self.cal_nozzle_z = config.getfloat("cal_nozzle_z", 0.1)
        self.cal_floor = config.getfloat("cal_floor", 0.2)
        self.cal_ceil = config.getfloat("cal_ceil", 5.0)
        self.cal_speed = config.getfloat("cal_speed", 1.0)
        self.cal_move_speed = config.getfloat("cal_move_speed", 10.0)

        self.autocal_max_speed = config.getfloat("autocal_max_speed", 10)
        self.autocal_speed = config.getfloat("autocal_speed", self.AUTOCAL_DEFAULT_SPEED)
        self.autocal_accel = config.getfloat("autocal_accel", self.AUTOCAL_DEFAULT_ACCEL)
        self.autocal_retract_dist = config.getfloat("autocal_retract_dist", self.AUTOCAL_DEFAULT_RETRACT)
        self.autocal_retract_speed = config.getfloat("autocal_retract_speed", self.AUTOCAL_DEFAULT_RETRACT_SPEED)
        self.autocal_sample_count = config.getfloat("autocal_sample_count", self.AUTOCAL_DEFAULT_SAMPLES)
        self.autocal_tolerance = config.getfloat("autocal_tolerance", self.AUTOCAL_DEFAULT_TOLERANCE)
        self.autocal_max_retries = config.getfloat("autocal_max_retries", self.AUTOCAL_DEFAULT_MAX_RETRIES)

        self.contact_latency_min = config.getint("contact_latency_min", 0)
        self.contact_sensitivity = config.getint("contact_sensitivity", 0)
        
        # NEW: Configurable samples for offset compare (Default 2 as requested)
        self.offset_compare_samples = config.getint("offset_compare_samples", 2, minval=1)

        self.skip_firmware_version_check = config.getboolean(
            "skip_firmware_version_check", False
        )
        
        self.hardware_failure = None
        
        # NEW: Safety Shim & Integrity State
        self._streaming_lock = False  # Prevents starting stream during critical ops
        self._last_packet_end_clock = 0
        self._packet_integrity_errors = 0

        # NEW: Safety Shim & Integrity State
        self._streaming_lock = False 
        self._last_packet_end_clock = 0
        self._packet_integrity_errors = 0
        
        # Load helper modules
        self.mesh_helper = BeaconMeshHelper.create(self, config)
        # ...

        # Load models
        self.model = None
        self.models = {}
        self.model_temp_builder = BeaconTempModelBuilder.load(config)
        self.model_temp = None
        self.fmin = None
        self.default_model_name = config.get("default_model_name", "default")
        self.model_manager = ModelManager(self)

        # Temperature sensor integration
        self.last_temp = 0
        self.last_mcu_temp = None
        self.measured_min = 99999999.0
        self.measured_max = 0.0
        self.mcu_temp = None
        self.thermistor = None

        # State variables
        self.last_sample = None
        self.last_received_sample = None
        self.last_z_result = 0
        self.last_probe_position = (0, 0)
        self.last_probe_result = None
        self.last_offset_result = None
        self.last_poke_result = None
        self.last_contact_msg = None
        self.hardware_failure = None

        # Load helper modules
        self.mesh_helper = BeaconMeshHelper.create(self, config)
        self.homing_helper = BeaconHomingHelper.create(self, config)
        self.accel_helper = None
        self.accel_config = BeaconAccelConfig(config)

        # Streaming state
        self._stream_en = 0
        self._stream_timeout_timer = self.reactor.register_timer(self._stream_timeout)
        self._stream_callbacks = {}
        self._stream_latency_requests = {}
        self._stream_buffer = []
        self._stream_buffer_count = 0
        self._stream_buffer_limit = STREAM_BUFFER_LIMIT_DEFAULT
        self._stream_buffer_limit_new = self._stream_buffer_limit
        self._stream_samples_queue = queue.Queue()
        self._stream_flush_event = threading.Event()
        self._log_stream = None
        self._data_filter = AlphaBetaFilter(
            config.getfloat("filter_alpha", 0.5),
            config.getfloat("filter_beta", 0.000001),
        )
        self.trapq = None
        self.mod_axis_twist_comp = None
        self.get_z_compensation_value = lambda pos: 0.0

        # MCU setup
        mainsync = printer.lookup_object("mcu")._clocksync
        self._mcu = MCU(config, SecondarySync(self.reactor, mainsync))
        orig_stats = self._mcu.stats

        def beacon_mcu_stats(eventtime):
            show, value = orig_stats(eventtime)
            value += " " + self._extend_stats()
            return show, value

        self._mcu.stats = beacon_mcu_stats
        printer.add_object(f"mcu {self.name}", self._mcu)
        self.cmd_queue = self._mcu.alloc_command_queue()
        
        # Endstop wrappers
        self._endstop_shared = BeaconEndstopShared(self)
        self.mcu_probe = BeaconEndstopWrapper(self)
        self.mcu_contact_probe = BeaconContactEndstopWrapper(self, config)
        self._current_probe = "proximity"

        # MCU command cache
        self.beacon_stream_cmd = None
        self.beacon_set_threshold = None
        self.beacon_home_cmd = None
        self.beacon_stop_home_cmd = None
        self.beacon_nvm_read_cmd = None
        self.beacon_contact_home_cmd = None
        self.beacon_contact_query_cmd = None
        self.beacon_contact_stop_home_cmd = None
        self.beacon_contact_set_latency_min_cmd = None
        self.beacon_contact_set_sensitivity_cmd = None

        # Register z_virtual_endstop
        register_as_probe = config.getboolean(
            "register_as_probe", sensor_id.is_unnamed()
        )
        if register_as_probe:
            printer.lookup_object("pins").register_chip("probe", self)

        # Register event handlers
        printer.register_event_handler("klippy:connect", self._handle_connect)
        printer.register_event_handler("klippy:shutdown", self.force_stop_streaming)
        self._mcu.register_config_callback(self._build_config)
        self._mcu.register_response(self._handle_beacon_data, "beacon_data")
        self._mcu.register_response(self._handle_beacon_status, "beacon_status")
        self._mcu.register_response(self._handle_beacon_contact, "beacon_contact")

        # Register webhooks
        self._api_dump = APIDumpHelper(
            printer,
            lambda: self.streaming_session(self._api_dump_callback, latency=50),
            lambda stream: stream.stop(),
            None,
        )
        sensor_id.register_endpoint("beacon/status", self._handle_req_status)
        sensor_id.register_endpoint("beacon/dump", self._handle_req_dump)

        # Register gcode commands
        sensor_id.register_command(
            "BEACON_STREAM", self.cmd_BEACON_STREAM, desc=self.cmd_BEACON_STREAM_help
        )
        sensor_id.register_command(
            "BEACON_QUERY", self.cmd_BEACON_QUERY, desc=self.cmd_BEACON_QUERY_help
        )
        sensor_id.register_command(
            "BEACON_CALIBRATE",
            self.cmd_BEACON_CALIBRATE,
            desc=self.cmd_BEACON_CALIBRATE_help,
        )
        sensor_id.register_command(
            "BEACON_ESTIMATE_BACKLASH",
            self.cmd_BEACON_ESTIMATE_BACKLASH,
            desc=self.cmd_BEACON_ESTIMATE_BACKLASH_help,
        )
        sensor_id.register_command("PROBE", self.cmd_PROBE, desc=self.cmd_PROBE_help)
        sensor_id.register_command(
            "PROBE_ACCURACY", self.cmd_PROBE_ACCURACY, desc=self.cmd_PROBE_ACCURACY_help
        )
        sensor_id.register_command(
            "Z_OFFSET_APPLY_PROBE",
            self.cmd_Z_OFFSET_APPLY_PROBE,
            desc=self.cmd_Z_OFFSET_APPLY_PROBE_help,
        )
        sensor_id.register_command(
            "BEACON_POKE", self.cmd_BEACON_POKE, desc=self.cmd_BEACON_POKE_help
        )
        sensor_id.register_command(
            "BEACON_AUTO_CALIBRATE",
            self.cmd_BEACON_AUTO_CALIBRATE,
            desc=self.cmd_BEACON_AUTO_CALIBRATE_help,
        )
        sensor_id.register_command(
            "BEACON_OFFSET_COMPARE",
            self.cmd_BEACON_OFFSET_COMPARE,
            desc=self.cmd_BEACON_OFFSET_COMPARE_help,
        )
        
        # Hook into other probing G-Codes if this is the default probe
        if sensor_id.is_unnamed():
            self._hook_gcode_commands(config)

    def _hook_gcode_commands(self, config):
        """
        Checks printer kinematics and registers hooks ONLY for
        compatible modules.
        """
        # Use the class attribute we set in __init__
        if self.is_delta:
            # --- Delta-Specific Checks ---
            if config.has_section("z_tilt"):
                raise config.error(
                    "beacon.py: [z_tilt] is not compatible with delta kinematics."
                )
            if config.has_section("quad_gantry_level"):
                raise config.error(
                    "beacon.py: [quad_gantry_level] is not compatible with delta kinematics."
                )
            if config.has_section("screws_tilt_adjust"):
                raise config.error(
                    "beacon.py: [screws_tilt_adjust] is not compatible with delta kinematics."
                )

            # This one IS compatible
            self._hook_probing_gcode(config, "delta_calibrate", "DELTA_CALIBRATE")

        else:
            # --- Cartesian/CoreXY Checks ---
            self._hook_probing_gcode(config, "z_tilt", "Z_TILT_ADJUST")
            self._hook_probing_gcode(config, "quad_gantry_level", "QUAD_GANTRY_LEVEL")
            self._hook_probing_gcode(config, "screws_tilt_adjust", "SCREWS_TILT_ADJUST")

    def _hook_probing_gcode(self, config, module, cmd):
        """
        Hooks into other Klipper modules that perform probing (e.g., Z_TILT)
        to set the correct probe method (contact/proximity).
        """
        if not config.has_section(module):
            return

        try:
            mod = self.printer.load_object(config, module)
        except self.printer.config_error as e:
            logging.info(f"Failed to load module {module} for hooking: {e}")
            return

        if mod is None:
            return

        original_handler = self.gcode.register_command(cmd, None)
        if original_handler is None:
            logging.info(f"Could not hook G-Code command {cmd}")
            return

        def hooked_cmd_callback(gcmd):
            self._current_probe = gcmd.get(
                "PROBE_METHOD", self.default_probe_method
            ).lower()
            return original_handler(gcmd)

        self.gcode.register_command(cmd, hooked_cmd_callback)

    # --- Klipper Event Handlers ---

    def _handle_connect(self):
        """
        Called when Klipper connects.
        Loads optional modules and sets default model.
        """
        self.phoming = self.printer.lookup_object("homing")
        self.mod_axis_twist_comp = self.printer.lookup_object(
            "axis_twist_compensation", None
        )
        if self.mod_axis_twist_comp:
            if hasattr(self.mod_axis_twist_comp, "get_z_compensation_value"):
                self.get_z_compensation_value = (
                    lambda pos: self.mod_axis_twist_comp.get_z_compensation_value(pos)
                )
            else:
                def _update_compensation(pos):
                    cpos = list(pos)
                    self.mod_axis_twist_comp._update_z_compensation_value(cpos)
                    return cpos[2] - pos[2]
                self.get_z_compensation_value = _update_compensation

        if self.model is None:
            self.model = self.models.get(self.default_model_name, None)

    def _handle_beacon_data(self, params):
        """
        Receives a batch of samples from the MCU.
        Includes Live Integrity Validation.
        """
        if self.trapq is None:
            return

        buf = bytearray(params["data"])
        sample_count = params["samples"]
        start_clock = params["start_clock"]
        delta_clock = (
            params["delta_clock"] / (sample_count - 1) if sample_count > 1 else 0
        )

        # INTEGRITY VALIDATOR: Check for gaps in MCU clock
        if self._last_packet_end_clock > 0:
            gap = start_clock - self._last_packet_end_clock
            # Threshold > 2 samples worth of time (approx 5000 ticks)
            threshold = delta_clock * 2 if delta_clock > 0 else 5000
            if gap > threshold:
                self._packet_integrity_errors += 1
                if self._packet_integrity_errors <= 5: # Reduce log spam
                    logging.warning(f"Beacon: Data Gap Detected! Gap: {gap} ticks. Total Errors: {self._packet_integrity_errors}")

        # Update end clock for next comparison
        self._last_packet_end_clock = start_clock + params["delta_clock"]

        samples = []
        data = 0
        
        idx = 0
        for i in range(0, sample_count):
            if buf[idx] & 0x80 == 0:
                # 2-byte delta
                delta = ((buf[idx] & 0x7F) << 8) + buf[idx+1]
                data = data + delta - ((buf[idx] & 0x40) << 9)
                idx += 2
            else:
                # 4-byte full value
                data = (buf[idx] & 0x7F) << 24 | buf[idx+1] << 16 | buf[idx+2] << 8 | buf[idx+3]
                idx += 4
            clock = start_clock + int(round(i * delta_clock))
            samples.append((clock, data))

        self._stream_buffer.append(samples)
        self._stream_buffer_count += len(samples)
        self._stream_flush_schedule()

    def _handle_beacon_status(self, params):
        """
        Receives a status update from the Beacon MCU (e.g., temps).
        """
        if self.mcu_temp is not None:
            self.last_mcu_temp = self.mcu_temp.compensate(
                self, params["mcu_temp"], params["supply_voltage"]
            )
        if self.thermistor is not None:
            self.last_temp = self.thermistor.calc_temp(
                params["coil_temp"] / self.temp_smooth_count * self.inv_adc_max
            )

    def _handle_beacon_contact(self, params):
        """
        Receives the contact trigger message.
        """
        self.last_contact_msg = params

    # --- Config & Startup ---

    def _build_config(self):
        """
        Called when the MCU connects, to look up and cache commands.
        """
        version_info = self._check_mcu_version()

        try:
            # Cache all MCU commands
            self.beacon_stream_cmd = self._mcu.lookup_command(
                "beacon_stream en=%u", cq=self.cmd_queue
            )
            self.beacon_set_threshold = self._mcu.lookup_command(
                "beacon_set_threshold trigger=%u untrigger=%u", cq=self.cmd_queue
            )
            self.beacon_home_cmd = self._mcu.lookup_command(
                "beacon_home trsync_oid=%c trigger_reason=%c trigger_invert=%c",
                cq=self.cmd_queue,
            )
            self.beacon_stop_home_cmd = self._mcu.lookup_command(
                "beacon_stop_home", cq=self.cmd_queue
            )
            self.beacon_nvm_read_cmd = self._mcu.lookup_query_command(
                "beacon_nvm_read len=%c offset=%hu",
                "beacon_nvm_data bytes=%*s offset=%hu",
                cq=self.cmd_queue,
            )
            self.beacon_contact_home_cmd = self._mcu.lookup_command(
                "beacon_contact_home trsync_oid=%c trigger_reason=%c trigger_type=%c",
                cq=self.cmd_queue,
            )
            self.beacon_contact_query_cmd = self._mcu.lookup_query_command(
                "beacon_contact_query",
                "beacon_contact_state triggered=%c detect_clock=%u",
                cq=self.cmd_queue,
            )
            self.beacon_contact_stop_home_cmd = self._mcu.lookup_command(
                "beacon_contact_stop_home",
                cq=self.cmd_queue,
            )
            try:
                self.beacon_contact_set_latency_min_cmd = self._mcu.lookup_command(
                    "beacon_contact_set_latency_min latency_min=%c",
                    cq=self.cmd_queue,
                )
            except msgproto.error:
                pass # Ignore if command doesn't exist
            try:
                self.beacon_contact_set_sensitivity_cmd = self._mcu.lookup_command(
                    "beacon_contact_set_sensitivity sensitivity=%c",
                    cq=self.cmd_queue,
                )
            except msgproto.error:
                pass # Ignore if command doesn't exist

            constants = self._mcu.get_constants()
            self._mcu_freq = self._mcu.get_constant_float("CLOCK_FREQ")

            # Setup thermistor for coil temperature
            self.inv_adc_max = 1.0 / constants.get("ADC_MAX")
            self.temp_smooth_count = constants.get("BEACON_ADC_SMOOTH_COUNT")
            self.thermistor = thermistor.Thermistor(10000.0, 0.0)
            self.thermistor.setup_coefficients_beta(25.0, 47000.0, 4101.0)

            # Get Klipper objects
            self.toolhead = self.printer.lookup_object("toolhead")
            self.kinematics = self.toolhead.get_kinematics()
            self.trapq = self.toolhead.get_trapq()

            # Load models from Beacon NVM
            self.mcu_temp = BeaconMCUTempHelper.build_with_nvm(self)
            self.model_temp = self.model_temp_builder.build_with_nvm(self)
            if self.model_temp:
                self.fmin = self.model_temp.fmin
            if self.model is None:
                self.model = self.models.get(self.default_model_name, None)
            if self.model:
                self._apply_threshold()

            # Re-enable streaming if it was active
            if self.beacon_stream_cmd is not None:
                self.beacon_stream_cmd.send([1 if self._stream_en else 0])
            if self._stream_en:
                curtime = self.reactor.monotonic()
                self.reactor.update_timer(
                    self._stream_timeout_timer, curtime + STREAM_TIMEOUT
                )
            else:
                self.reactor.update_timer(
                    self._stream_timeout_timer, self.reactor.NEVER
                )

            # Enable accelerometer if present
            if constants.get("BEACON_HAS_ACCEL", 0) == 1:
                logging.info("Enabling Beacon accelerometer")
                if self.accel_helper is None:
                    self.accel_helper = BeaconAccelHelper(
                        self, self.accel_config, constants
                    )
                else:
                    self.accel_helper.reinit(constants)

        except msgproto.error as e:
            if version_info != "":
                raise msgproto.error(f"{version_info}\n\n{e}")
            raise

    def _check_mcu_version(self):
        """
        Runs the update_firmware.py script to check for outdated FW.
        """
        if self.skip_firmware_version_check:
            return ""
        
        updater_script = os.path.join(self.id.tracker.home_dir(), self.FIRMWARE_UPDATE_SCRIPT)
        if not os.path.exists(updater_script):
            logging.info("Could not find Beacon firmware update script, won't check for update.")
            return ""
            
        serial_port = self.compat_serial_port(self._mcu)

        parent_conn, child_conn = multiprocessing.Pipe()

        def do_check_in_subprocess():
            try:
                output = subprocess.check_output(
                    [updater_script, "check", serial_port], universal_newlines=True
                )
                child_conn.send((False, output.strip()))
            except (subprocess.CalledProcessError, OSError) as e:
                child_conn.send((True, traceback.format_exc()))
            finally:
                child_conn.close()

        child = multiprocessing.Process(target=do_check_in_subprocess)
        child.daemon = True
        child.start()
        
        eventtime = self.reactor.monotonic()
        while child.is_alive():
            eventtime = self.reactor.pause(eventtime + 0.1)
            
        (is_err, result) = parent_conn.recv()
        child.join()
        parent_conn.close()
        
        if is_err:
            logging.info(f"Executing Beacon update script failed: {result}")
        elif result != "":
            self.gcode.respond_raw(f"!! {result}\n")
            pconfig = self.printer.lookup_object("configfile")
            try:
                pconfig.runtime_warning(result)
            except AttributeError:
                logging.info(result)
            return result
        return ""

    # --- G-Code Command Handlers ---

    cmd_PROBE_help = "Probe Z-height at current XY position"
    def cmd_PROBE(self, gcmd):
        self.last_probe_result = "failed"
        pos = self.run_probe(gcmd)
        gcmd.respond_info(f"Result is z={pos[2]:.6f}")
        offset = self.get_offsets()
        self.last_z_result = pos[2] - offset[2]
        self.last_probe_position = (pos[0] - offset[0], pos[1] - offset[1])
        self.last_probe_result = "ok"

    cmd_BEACON_CALIBRATE_help = "Calibrate beacon response curve"
    def cmd_BEACON_CALIBRATE(self, gcmd):
        self._start_calibration(gcmd)

    cmd_BEACON_ESTIMATE_BACKLASH_help = "Estimate Z axis backlash"
    def cmd_BEACON_ESTIMATE_BACKLASH(self, gcmd):
        overrun = gcmd.get_float("OVERRUN", 0.5)
        target_z_dist = gcmd.get_float("Z", 1.5)
        speed = gcmd.get_float("SPEED", 100.0) # Fast approach
        creep_speed = gcmd.get_float("CREEP", 20.0) # Slow approach
        
        num_samples = gcmd.get_int("SAMPLES", 20)
        sample_count_per_read = 50
        settle_time = self.z_settling_time
        dwell_time = 0.1
        
        cycle_speed = 30.0

        if target_z_dist - overrun < 0.5:
            raise gcmd.error(
                f"Test target ({target_z_dist}) minus overrun ({overrun}) is too close to bed! (Min safe height is 0.5mm)"
            )

        # 1. Home
        gcmd.respond_info("Homing...")
        self.gcode.run_script_from_command("G28")
        
        # 2. Move to Safe Z (Fast)
        start_pos_z = target_z_dist + overrun
        gcmd.respond_info(f"Moving to start position Z={start_pos_z:.3f} at {speed}mm/s")
        self.toolhead.manual_move([None, None, start_pos_z], speed)
        self.toolhead.wait_moves()

        # 3. Slower move from safety offset site to testing site
        gcmd.respond_info(f"Approaching target Z={target_z_dist:.3f} at {creep_speed}mm/s")
        self.toolhead.manual_move([None, None, target_z_dist], creep_speed)
        self.toolhead.wait_moves()
        self.toolhead.dwell(1.0)
        
        (baseline_dist, _) = self._sample(settle_time, sample_count_per_read)
        target_kin_z = self.toolhead.get_position()[2]
        gcmd.respond_info(f"Baseline: {baseline_dist:.5f} at Machine Z: {target_kin_z:.4f}")

        samples_down = []
        samples_up = []

        # 4. Test Loop
        self.request_stream_latency(50)
        self._start_streaming()
        
        try:
            for i in range(num_samples):
                # --- DOWN SAMPLE ---
                up_pos = target_kin_z + overrun
                self.toolhead.manual_move([None, None, up_pos], cycle_speed)
                self.toolhead.manual_move([None, None, target_kin_z], creep_speed)
                self.toolhead.wait_moves()
                self.toolhead.dwell(dwell_time)
                
                (dist_down, _) = self._sample(0, sample_count_per_read)
                samples_down.append(dist_down)
                
                # --- UP SAMPLE ---
                low_pos = target_kin_z - overrun
                if low_pos < 0.1: low_pos = 0.1
                self.toolhead.manual_move([None, None, low_pos], cycle_speed)
                self.toolhead.manual_move([None, None, target_kin_z], creep_speed)
                self.toolhead.wait_moves()
                self.toolhead.dwell(dwell_time)
                
                (dist_up, _) = self._sample(0, sample_count_per_read)
                samples_up.append(dist_up)

                diff = dist_up - dist_down
                gcmd.respond_info(
                    f"Sample {i+1}/{num_samples}: Down={dist_down:.5f}, Up={dist_up:.5f}, Backlash={diff:.5f}"
                )

        finally:
            self._stop_streaming()
            self.drop_stream_latency_request(50)

        # 5. Finish Safe
        self.toolhead.manual_move([None, None, 5.0], speed)
        self.gcode.run_script_from_command("BED_MESH_CLEAR") # Prevent NaN crash
        self.gcode.run_script_from_command("G28")

        # Stats
        avg_down = np.mean(samples_down)
        avg_up = np.mean(samples_up)
        median_backlash = median([u - d for u, d in zip(samples_up, samples_down)])
        avg_backlash = avg_up - avg_down

        gcmd.respond_info(
            f"Backlash Results ({num_samples} cycles):\n"
            f"Avg Down (Push): {avg_down:.5f}\n"
            f"Avg Up (Pull):   {avg_up:.5f}\n"
            f"Avg Backlash:    {avg_backlash:.5f}\n"
            f"Median Backlash: {median_backlash:.5f}"
        )

    cmd_BEACON_QUERY_help = "Take a single sample from the sensor"
    def cmd_BEACON_QUERY(self, gcmd):
        sample = self._sample_async()
        last_freq = sample["freq"]
        dist = sample["dist"]
        temp = sample["temp"]
        self.last_sample = {
            "time": sample["time"],
            "value": last_freq,
            "temp": temp,
            "dist": None if dist is None or np.isinf(dist) or np.isnan(dist) else dist,
        }
        if dist is None:
            gcmd.respond_info(f"Last reading: {last_freq:.2f}Hz, {temp:.2f}C, no model")
        else:
            gcmd.respond_info(f"Last reading: {last_freq:.2f}Hz, {temp:.2f}C, {dist:.5f}mm")

    cmd_BEACON_STREAM_help = "Stream Beacon data to a file"
    def cmd_BEACON_STREAM(self, gcmd):
        if self._log_stream is not None:
            self._log_stream.stop()
            self._log_stream = None
            gcmd.respond_info("Beacon Streaming disabled")
        else:
            filename = gcmd.get("FILENAME")
            try:
                file_handle = open(filename, "w")
            except OSError as e:
                raise gcmd.error(f"Failed to open file '{filename}': {e}")

            def close_file():
                file_handle.close()

            file_handle.write("time,data,data_smooth,freq,dist,temp,pos_x,pos_y,pos_z\n")

            def _stream_to_file_callback(sample):
                pos = sample.get("pos", None)
                pos_x_str = f"{pos[0]:.3f}" if pos is not None else ""
                pos_y_str = f"{pos[1]:.3f}" if pos is not None else ""
                pos_z_str = f"{pos[2]:.3f}" if pos is not None else ""
                
                log_line = (
                    f"{sample['time']:.4f},{sample['data']},{sample['data_smooth']:.2f},"
                    f"{sample['freq']:.5f},{sample['dist']:.5f},{sample['temp']:.2f},"
                    f"{pos_x_str},{pos_y_str},{pos_z_str}\n"
                )
                file_handle.write(log_line)

            self._log_stream = self.streaming_session(_stream_to_file_callback, close_file)
            gcmd.respond_info(f"Beacon Streaming enabled, writing to {filename}")

    cmd_PROBE_ACCURACY_help = "Probe Z-height accuracy at current XY position"
    def cmd_PROBE_ACCURACY(self, gcmd):
        speed = gcmd.get_float("PROBE_SPEED", self.speed, above=0.0)
        lift_speed = self.get_lift_speed(gcmd)
        sample_count = gcmd.get_int("SAMPLES", 10, minval=1)
        sample_retract_dist = gcmd.get_float("SAMPLE_RETRACT_DIST", 0)
        allow_faulty = gcmd.get_int("ALLOW_FAULTY_COORDINATE", 0) != 0
        
        pos = self.toolhead.get_position()
        gcmd.respond_info(
            f"PROBE_ACCURACY at X:{pos[0]:.3f} Y:{pos[1]:.3f} Z:{pos[2]:.3f} "
            f"(samples={sample_count} retract={sample_retract_dist:.3f} "
            f"speed={speed:.1f} lift_speed={lift_speed:.1f})\n"
        )

        start_height = self.trigger_distance + sample_retract_dist
        lift_pos = [None, None, start_height]
        self.toolhead.manual_move(lift_pos, lift_speed)

        self.multi_probe_begin()
        positions = []
        try:
            while len(positions) < sample_count:
                pos = self._probe(speed, allow_faulty=allow_faulty)
                positions.append(pos)
                self.toolhead.manual_move(lift_pos, lift_speed)
        finally:
            self.multi_probe_end()

        z_positions = [p[2] for p in positions]
        max_z = max(z_positions)
        min_z = min(z_positions)
        range_z = max_z - min_z
        avg_z = sum(z_positions) / len(positions)
        median_z = median(z_positions)

        deviation_sum = sum([(z - avg_z) ** 2 for z in z_positions])
        sigma = (deviation_sum / len(z_positions)) ** 0.5

        gcmd.respond_info(
            f"probe accuracy results: maximum {max_z:.6f}, minimum {min_z:.6f}, range {range_z:.6f}, "
            f"average {avg_z:.6f}, median {median_z:.6f}, standard deviation {sigma:.6f}"
        )

    cmd_Z_OFFSET_APPLY_PROBE_help = "Adjust the probe's z_offset"
    def cmd_Z_OFFSET_APPLY_PROBE(self, gcmd):
        gcode_move = self.printer.lookup_object("gcode_move")
        z_offset = gcode_move.get_status()["homing_origin"].z

        if z_offset == 0:
            self.gcode.respond_info("Nothing to do: Z Offset is 0")
            return

        if not self.model:
            raise self.gcode.error("You must calibrate your model first, use BEACON_CALIBRATE.")

        # See BeaconModel.save() for notes on this logic
        old_offset = self.model.offset
        self.model.offset += z_offset
        self.model.save(self, False)
        gcmd.respond_info(
            f"Beacon model offset has been updated, new value is {self.model.offset:.5f}\n"
            "You must run the SAVE_CONFIG command now to update the\n"
            "printer config file and restart the printer."
        )
        self.model.offset = old_offset

    cmd_BEACON_POKE_help = "Test contact probe by poking the bed"
    def cmd_BEACON_POKE(self, gcmd):
        top = gcmd.get_float("TOP", self.POKE_DEFAULT_TOP)
        bottom = gcmd.get_float("BOTTOM", self.POKE_DEFAULT_BOTTOM)
        speed = gcmd.get_float("SPEED", self.POKE_DEFAULT_SPEED, maxval=self.autocal_max_speed)

        pos = self.toolhead.get_position()
        gcmd.respond_info(
            f"Poke test at ({pos[0]:.3f},{pos[1]:.3f}), from {top:.3f} to {bottom:.3f}, at {speed:.3f} mm/s"
        )

        self.last_probe_result = "failed"
        self.toolhead.manual_move([None, None, top], 100.0)
        self.toolhead.wait_moves()
        self.toolhead.dwell(0.5)

        ts = time.strftime("%Y%m%d_%H%M%S")
        filename = f"/tmp/poke_{ts}_{speed:.3f}_{top:.3f}_{bottom:.3f}.csv"
        
        try:
            with open(filename, "w") as f:
                f.write("time,data,data_smooth,freq,dist,temp,pos_x,pos_y,pos_z\n")

                def _poke_stream_callback(sample):
                    pos = sample.get("pos", None)
                    pos_x_str = f"{pos[0]:.3f}" if pos is not None else ""
                    pos_y_str = f"{pos[1]:.3f}" if pos is not None else ""
                    pos_z_str = f"{pos[2]:.5f}" if pos is not None else ""
                    
                    log_line = (
                        f"{sample['time']:.6f},{sample['data']},{sample['data_smooth']:.2f},"
                        f"{sample['freq']:.5f},{sample['dist']:.5f},{sample['temp']:.2f},"
                        f"{pos_x_str},{pos_y_str},{pos_z_str}\n"
                    )
                    f.write(log_line)

                # OPTIMIZATION: 50ms latency allows for better contact resolution
                self.request_stream_latency(50)
                self._start_streaming()

                with self.streaming_session(_poke_stream_callback, latency=50):
                    self._sample_async()
                    self.toolhead.get_last_move_time()
                    pos = self.toolhead.get_position()
                    self.mcu_contact_probe.activate_gcode.run_gcode_from_command()
                    try:
                        hmove = HomingMove(
                            self.printer, [(self.mcu_contact_probe, "contact")]
                        )
                        pos[2] = bottom
                        epos = hmove.homing_move(pos, speed, probe_pos=True)[:3]
                        self.toolhead.wait_moves()
                        spos = self.toolhead.get_position()[:3]
                        armpos = self._get_position_at_time(
                            self._clock32_to_time(self.last_contact_msg["armed_clock"])
                        )
                        gcmd.respond_info(f"Armed at:     z={armpos[2]:.5f}")
                        gcmd.respond_info(
                            f"Triggered at: z={epos[2]:.5f} with latency={self.last_contact_msg['latency']}"
                        )
                        gcmd.respond_info(f"Overshoot:    {(epos[2] - spos[2]) * 1000.0:.3f} um")
                        
                        self.last_probe_result = "ok"
                        self.last_poke_result = {
                            "target_position": pos,
                            "arming_z": armpos[2],
                            "trigger_z": epos[2],
                            "stopped_z": spos[2],
                            "latency": self.last_contact_msg["latency"],
                            "error": self.last_contact_msg["error"],
                        }
                    except self.printer.command_error:
                        if self.printer.is_shutdown():
                            raise self.printer.command_error(
                                "Homing failed due to printer shutdown"
                            )
                        raise
                    finally:
                        self.mcu_contact_probe.deactivate_gcode.run_gcode_from_command()
                        self.toolhead.manual_move([None, None, top], 100.0)
                        self.toolhead.wait_moves()
                        # Clean up
                        self._stop_streaming()
                        self.drop_stream_latency_request(50)

        except OSError as e:
            gcmd.respond_info(f"Warning: Could not write poke data to {filename}: {e}")
            
    cmd_BEACON_AUTO_CALIBRATE_help = "Automatically calibrates the Beacon probe"
    def cmd_BEACON_AUTO_CALIBRATE(self, gcmd):
        speed = gcmd.get_float("SPEED", self.autocal_speed, above=0, maxval=self.autocal_max_speed)
        desired_accel = gcmd.get_float("ACCEL", self.autocal_accel, minval=1)
        retract_dist = gcmd.get_float("RETRACT", self.autocal_retract_dist, minval=1)
        retract_speed = gcmd.get_float("RETRACT_SPEED", self.autocal_retract_speed, minval=1)
        sample_count = gcmd.get_int("SAMPLES", self.autocal_sample_count, minval=1)
        tolerance = gcmd.get_float("SAMPLES_TOLERANCE", self.autocal_tolerance, above=0.0)
        max_retries = gcmd.get_int("SAMPLES_TOLERANCE_RETRIES", self.autocal_max_retries, minval=0)
        
        self.cal_speed = speed
        
        curtime = self.reactor.monotonic()
        kin_status = self.kinematics.get_status(curtime)
        
        if "x" not in kin_status["homed_axes"] or "y" not in kin_status["homed_axes"] or "z" not in kin_status["homed_axes"]:
             gcmd.respond_info("Printer not homed. Homing now...")
             self.gcode.run_script_from_command("G28")
        
        # 1. Move to Safe Z (2.0mm) at 100mm/s
        self.toolhead.manual_move([None, None, 2.0], 100.0)
        self.toolhead.wait_moves()

        home_pos = self.toolhead.get_position()
        home_pos[2] = self.CONTACT_PROBE_TARGET_Z 
        
        stop_samples = []
        old_max_accel = self.toolhead.get_status(curtime)["max_accel"]
        
        def set_max_accel(value):
            self.gcode.run_script_from_command(f"SET_VELOCITY_LIMIT ACCEL={value:.3f}")
            
        self.mcu_contact_probe.activate_gcode.run_gcode_from_command()
        
        try:
            skip_next = True
            retries = 0
            while len(stop_samples) < sample_count:
                if skip_next: 
                    gcmd.respond_info("Initial approach")
                else: 
                    gcmd.respond_info(f"Collecting sample {len(stop_samples) + 1}/{int(sample_count)}")
                
                self.toolhead.wait_moves()
                set_max_accel(desired_accel)
                
                try:
                    hmove = HomingMove(self.printer, [(self.mcu_contact_probe, "contact")])
                    epos = hmove.homing_move(home_pos, speed, probe_pos=True)
                except self.printer.command_error:
                    if self.printer.is_shutdown(): raise self.printer.command_error("Homing failed due to printer shutdown")
                    raise
                finally:
                    set_max_accel(old_max_accel)
                
                retract_pos = self.toolhead.get_position()[:]
                retract_pos[2] += retract_dist
                self.toolhead.move(retract_pos, retract_speed)
                self.toolhead.dwell(1.0) 
                
                if not skip_next:
                    stop_samples.append(epos[2])
                    mean = np.mean(stop_samples)
                    sd = np.std(stop_samples) if len(stop_samples) > 1 else 0.0
                    
                    delta = max([abs(v - mean) for v in stop_samples])
                    if delta > tolerance:
                        if retries >= max_retries: 
                            raise gcmd.error(f"Calibration tolerance exceeded: range {delta:.4f} > {tolerance}")
                        gcmd.respond_info(f"Tolerance exceeded ({delta:.4f}), retrying...")
                        stop_samples = []
                        retries += 1
                        skip_next = True
                    else:
                         if len(stop_samples) > 1:
                             gcmd.respond_info(f"Sample collected. Mean: {mean:.5f}, SD: {sd:.5f}")
                else:
                    skip_next = False

            z_zero = np.mean(stop_samples)
            gcmd.respond_info(f"Contact zero found at {z_zero:.5f}")
            gcmd.respond_info("Contact phase complete. Starting Beacon scan...")
            
            # --- FIX START ---
            # 1. Sync the toolhead Z to the contact result (make Z relative to bed)
            cur_pos = self.toolhead.get_position()
            # The toolhead is currently at (z_zero + retract_dist). 
            # We tell Klipper that physical Z is actually (current_Z - z_zero).
            # Actually, simpler: Just reset Z to the retract height relative to 0.
            self.compat_toolhead_set_position_homing_z(self.toolhead, [cur_pos[0], cur_pos[1], retract_dist])
            
            # 2. Move to the Calibration Start Height (cal_nozzle_z, usually 0.1mm)
            # We must be PHYSICALLY at 0.1mm before _calibrate calls set_position(0.1)
            gcmd.respond_info(f"Moving to calibration start height: {self.cal_nozzle_z}mm")
            self.toolhead.manual_move([None, None, self.cal_nozzle_z], self.lift_speed)
            self.toolhead.wait_moves()
            
            # 3. Start Scan (Skip manual probe)
            self._start_calibration(gcmd, skip_manual_probe=True)

        finally:
            self.mcu_contact_probe.deactivate_gcode.run_gcode_from_command()
        
        # 3. Final Home (Delta Best Practice)
        gcmd.respond_info("Auto Calibration complete. Homing...")
        # SAFETY: Clear Mesh before homing to avoid NaN injection from edges
        self.gcode.run_script_from_command("BED_MESH_CLEAR")
        self.gcode.run_script_from_command("G28")
        

    cmd_BEACON_OFFSET_COMPARE_help = "Compare contact and proximity offsets"
    def cmd_BEACON_OFFSET_COMPARE(self, gcmd):
        # 1. Homing Check
        curtime = self.reactor.monotonic()
        kin_status = self.kinematics.get_status(curtime)
        if "x" not in kin_status["homed_axes"] or "y" not in kin_status["homed_axes"] or "z" not in kin_status["homed_axes"]:
             gcmd.respond_info("Printer not homed. Homing now...")
             self.gcode.run_script_from_command("G28")

        start_pos = self.toolhead.get_position()
        # Use config value (default 2)
        num_samples = gcmd.get_int("SAMPLES", self.offset_compare_samples)
        
        # 2. Move to Safe Z (2.0mm)
        self.toolhead.manual_move([None, None, 2.0], 50.0)
        self.toolhead.wait_moves()

        deltas = []

        for i in range(num_samples):
            if num_samples == 1:
                gcmd.respond_info("Probing with Contact (Speed 3.0)...")
            else:
                gcmd.respond_info(f"Offset Compare Sample {i+1}/{num_samples}")

            self.mcu_contact_probe.activate_gcode.run_gcode_from_command()
            try:
                hmove = HomingMove(self.printer, [(self.mcu_contact_probe, "contact")])
                # Pass current X/Y explicitly
                current_xy = self.toolhead.get_position()
                target_pos = [current_xy[0], current_xy[1], -2.0]
                
                epos = hmove.homing_move(target_pos, 3.0, probe_pos=True) 
                contact_z = epos[2]
            finally:
                self.mcu_contact_probe.deactivate_gcode.run_gcode_from_command()

            # 3. Retract and measure
            measure_z = contact_z + 3.0
            self.toolhead.manual_move([None, None, measure_z], 10.0)
            self.toolhead.wait_moves()
            self.toolhead.dwell(0.5)
            
            # 4. Measure Proximity
            self.request_stream_latency(50)
            self._start_streaming()
            (dist, samples) = self._sample(self.z_settling_time, 50)
            self._stop_streaming()
            self.drop_stream_latency_request(50)
            
            proximity_z = measure_z - dist 
            delta = contact_z - proximity_z 
            deltas.append(delta)
            
            gcmd.respond_info(
                f"Sample {i+1}: Contact={contact_z:.5f}, Proximity={proximity_z:.5f}, Delta={delta * 1000.0:.3f} um"
            )

        # Stats
        if num_samples > 1:
            avg_delta = np.mean(deltas)
            median_delta = median(deltas)
            sd_delta = np.std(deltas)
            range_delta = max(deltas) - min(deltas)
            
            gcmd.respond_info(
                f"Offset Compare Results ({num_samples} samples):\n"
                f"Mean Delta:   {avg_delta * 1000.0:.3f} um\n"
                f"Median Delta: {median_delta * 1000.0:.3f} um\n"
                f"Std Dev:      {sd_delta * 1000.0:.3f} um\n"
                f"Range:        {range_delta * 1000.0:.3f} um"
            )
        
        # 5. Cleanup
        gcmd.respond_info("Test complete. Cleaning up...")
        self.toolhead.manual_move([None, None, 2.0], 50.0)
        if start_pos is not None:
             self.toolhead.manual_move([start_pos[0], start_pos[1], None], 50.0)
        self.toolhead.wait_moves()
        self.gcode.run_script_from_command("G28")

    # --- Core Probing Logic ---

    def _start_calibration(self, gcmd, skip_manual_probe=False):
        """
        Starts the model calibration process, handling manual probe if needed.
        """
        allow_faulty = gcmd.get_int("ALLOW_FAULTY_COORDINATE", 0) != 0
        nozzle_z = gcmd.get_float("NOZZLE_Z", self.cal_nozzle_z)
        
        if skip_manual_probe or gcmd.get("SKIP_MANUAL_PROBE", None) is not None:
            kin = self.kinematics
            kin_spos = {
                s.get_name(): s.get_commanded_position() for s in kin.get_steppers()
            }
            kin_pos = kin.calc_position(kin_spos)
            if self._is_faulty_coordinate(kin_pos[0], kin_pos[1]):
                msg = "Calibrating within a faulty area"
                if not allow_faulty:
                    raise gcmd.error(msg)
                else:
                    gcmd.respond_raw(f"!! {msg}\n")
            self._calibrate(gcmd, kin_pos, nozzle_z, False, is_auto=True)
        else:
            curtime = self.printer.get_reactor().monotonic()
            kin_status = self.toolhead.get_status(curtime)
            if "xy" not in kin_status["homed_axes"]:
                raise self.printer.command_error("Must home X and Y before calibration")

            kin_pos = self.toolhead.get_position()
            if self._is_faulty_coordinate(kin_pos[0], kin_pos[1]):
                msg = "Calibrating within a faulty area"
                if not allow_faulty:
                    raise gcmd.error(msg)
                else:
                    gcmd.respond_raw(f"!! {msg}\n")

            forced_z = False
            if "z" not in kin_status["homed_axes"]:
                self.toolhead.get_last_move_time()
                pos = self.toolhead.get_position()
                pos[2] = (
                    kin_status["axis_maximum"][2]
                    - 2.0
                    - gcmd.get_float("CEIL", self.cal_ceil)
                )
                self.compat_toolhead_set_position_homing_z(self.toolhead, pos)
                forced_z = True

            def manual_probe_callback(kin_pos):
                return self._calibrate(gcmd, kin_pos, nozzle_z, forced_z)

            manual_probe.ManualProbeHelper(self.printer, gcmd, manual_probe_callback)

    def _calibrate(self, gcmd, kin_pos, cal_nozzle_z, forced_z, is_auto=False):
        """
        Internal calibration routine. Moves the probe down
        while streaming to build a frequency-vs-distance model.
        """
        if kin_pos is None:
            if forced_z:
                kin = self.kinematics
                self.compat_kin_note_z_not_homed(kin)
            return

        gcmd.respond_info("Beacon calibration starting")
        cal_floor = gcmd.get_float("FLOOR", self.cal_floor)
        cal_ceil = gcmd.get_float("CEIL", self.cal_ceil)
        cal_min_z = kin_pos[2] - cal_nozzle_z + cal_floor
        cal_max_z = kin_pos[2] - cal_nozzle_z + cal_ceil
        cal_speed = gcmd.get_float("DESCEND_SPEED", self.cal_speed)
        move_speed = gcmd.get_float("MOVE_SPEED", self.cal_move_speed)
        model_name = gcmd.get("MODEL_NAME", "default")

        toolhead = self.toolhead
        toolhead.wait_moves()

        # Move coordinate system to nozzle location
        self.toolhead.get_last_move_time()
        current_pos = toolhead.get_position()
        current_pos[2] = cal_nozzle_z
        toolhead.set_position(current_pos)

        # Move over to probe coordinate and remove Z backlash
        current_pos[2] = cal_max_z + self.backlash_comp
        toolhead.manual_move(current_pos, move_speed)  # Move Up
        current_pos[0] -= self.x_offset
        current_pos[1] -= self.y_offset
        toolhead.manual_move(current_pos, move_speed)  # Move Over
        current_pos[2] = cal_max_z
        toolhead.manual_move(current_pos, move_speed)  # Move Down
        toolhead.wait_moves()

        samples = []
        def _calibrate_callback(sample):
            samples.append(sample)
        
        # Descend while sampling
        toolhead.flush_step_generation()
        try:
            self._start_streaming()
            self._sample_printtime_sync(self.CALIBRATION_SAMPLE_SYNC_COUNT)
            with self.streaming_session(_calibrate_callback):
                self._sample_printtime_sync(self.CALIBRATION_SAMPLE_SYNC_COUNT)
                toolhead.dwell(self.CALIBRATION_DWELL_TIME)
                current_pos[2] = cal_min_z
                toolhead.manual_move(current_pos, cal_speed)
                toolhead.flush_step_generation()
                self._sample_printtime_sync(self.CALIBRATION_SAMPLE_SYNC_COUNT)
        finally:
            self._stop_streaming()

        # Fit the sampled data to a polynomial
        z_offset = [s["pos"][2] - cal_min_z + cal_floor for s in samples]
        freq = [s["freq"] for s in samples]
        temp = [s["temp"] for s in samples]
        inv_freq = [1 / f for f in freq]
        poly = Polynomial.fit(inv_freq, z_offset, self.CALIBRATION_POLY_DEGREE)
        temp_median = median(temp)
        
        # Save the new model
        self.model = BeaconModel(
            model_name, self, poly, temp_median, min(z_offset), max(z_offset)
        )
        self.models[self.model.name] = self.model
        self.model.save(self, not is_auto)
        self._apply_threshold()

        self.toolhead.get_last_move_time()
        final_pos = self.toolhead.get_position()
        final_pos[2] = cal_floor
        self.toolhead.set_position(final_pos)

        # Dump calibration curve for debugging
        filename = f"/tmp/beacon-calibrate-{time.strftime('%Y%M%d_%H%M%S')}.csv"
        try:
            with open(filename, "w") as f:
                f.write("freq,z,temp\n")
                for i in range(len(freq)):
                    f.write(f"{freq[i]:.5f},{z_offset[i]:.5f},{temp[i]:.3f}\n")
        except OSError as e:
            logging.info(f"Failed to write calibration data to {filename}: {e}")

        gcmd.respond_info(
            f"Beacon calibrated at {final_pos[0]:.3f},{final_pos[1]:.3f} from "
            f"{cal_min_z:.3f} to {cal_max_z:.3f}, speed {cal_speed:.2f} mm/s, "
            f"temp {temp_median:.2f}C"
        )

    def _run_probe_proximity(self, gcmd):
        """
        Performs a single proximity (non-contact) probe.
        """
        if self.model is None:
            raise self.printer.command_error("No Beacon model loaded")

        speed = gcmd.get_float("PROBE_SPEED", self.speed, above=0.0)
        allow_faulty = gcmd.get_int("ALLOW_FAULTY_COORDINATE", 0) != 0
        toolhead = self.printer.lookup_object("toolhead")
        curtime = self.reactor.monotonic()
        if "z" not in toolhead.get_status(curtime)["homed_axes"]:
            raise self.printer.command_error("Must home before probe")

        self._start_streaming()
        try:
            return self._probe(speed, allow_faulty=allow_faulty)
        finally:
            self._stop_streaming()

    def _probe(self, speed, 
               num_samples=PROBE_DEFAULT_SAMPLE_COUNT, 
               allow_faulty=False):
        """
        Internal proximity probe logic.
        Measures distance and performs a dive if too high.
        """
        target_dist = self.trigger_distance
        dive_threshold = self.trigger_dive_threshold
        (dist, samples) = self._sample(self.PROBE_DEFAULT_SKIP_SAMPLES, num_samples)

        x, y = samples[0]["pos"][0:2]
        if self._is_faulty_coordinate(x, y, add_offsets=True):
            msg = "Probing within a faulty area"
            if not allow_faulty:
                raise self.printer.command_error(msg)
            else:
                self.gcode.respond_raw(f"!! {msg}\n")

        if dist > target_dist + dive_threshold:
            # We are too high, must perform a homing-style probe move down
            
            # --- BUG FIX ---
            # We must stop streaming *before* calling a homing-based move
            # to avoid a mode conflict on the Beacon MCU.
            self._stop_streaming()
            
            self._probing_move_to_probing_height(speed)
            
            # Restart streaming and take a new sample
            self._start_streaming()
            (dist, samples) = self._sample(self.z_settling_time, num_samples)
            # --- END FIX ---
            
        elif math.isinf(dist) and dist < 0:
            # We are below the valid model range
            msg = "Attempted to probe with Beacon below calibrated model range"
            raise self.printer.command_error(msg)
        elif self.toolhead.get_position()[2] < target_dist - dive_threshold:
            # We are too low, move up to probing height and re-measure
            self._move_to_probing_height(speed)
            (dist, samples) = self._sample(self.z_settling_time, num_samples)

        pos = samples[0]["pos"]
        self.gcode.respond_info(f"probe at {pos[0]:.3f},{pos[1]:.3f},{pos[2]:.3f} is z={dist:.6f}")

        # Return [X, Y, Z_Result]
        return [pos[0], pos[1], pos[2] + target_dist - dist]

    def _probing_move_to_probing_height(self, speed):
        # ... (existing setup code) ...
        
        target_z = self.trigger_distance
        
        # FIX: Deltas crash if we use the heavy 'probing_move' while streaming.
        if self.is_delta:
             # Delta Movement Safety:
             # Ensure we never descend into unsafe delta configurations.
             # If current Z is lower than target, raise to target first.
             self.toolhead.wait_moves() 
             
             cur_pos = self.toolhead.get_position()
             # If we are currently LOWER than the target (e.g. near bed), move UP first.
             if cur_pos[2] < target_z:
                 self.toolhead.manual_move([None, None, target_z], speed)
                 self.toolhead.wait_moves()
                 
             # Now perform the move to target height.
             # If we were high up, this comes down. If we were low, we are already there.
             self.toolhead.manual_move([None, None, target_z], speed)
             self.toolhead.wait_moves()
        else:
            # ... (Keep existing Cartesian/CoreXY logic) ...
            # Standard behavior for Cartesian/CoreXY
            pos[2] = status["axis_minimum"][2]
            try:
                self.phoming.probing_move(self.mcu_probe, pos, speed)
            except self.printer.command_error as e:
                reason = str(e)
                if "Timeout during probing move" in reason:
                    reason += probe.HINT_TIMEOUT
                raise self.printer.command_error(reason)

    def _run_probe_contact(self, gcmd):
        """
        Performs a contact probe, averaging multiple samples.
        """
        self.toolhead.wait_moves()
        speed = gcmd.get_float(
            "PROBE_SPEED", self.autocal_speed, above=0.0, maxval=self.autocal_max_speed
        )
        lift_speed = self.get_lift_speed(gcmd)
        sample_count = gcmd.get_int("SAMPLES", self.autocal_sample_count, minval=1)
        retract_dist = gcmd.get_float(
            "SAMPLE_RETRACT_DIST", self.autocal_retract_dist, minval=1.0
        )
        tolerance = gcmd.get_float(
            "SAMPLES_TOLERANCE", self.autocal_tolerance, above=0.0
        )
        max_retries = gcmd.get_int(
            "SAMPLES_TOLERANCE_RETRIES", self.autocal_max_retries, minval=0
        )
        samples_result = gcmd.get("SAMPLES_RESULT", "mean")
        drop_n = gcmd.get_int("SAMPLES_DROP", 0, minval=0)
        
        retries = 0
        samples = []
        pos_xy = self.toolhead.get_position()[:2]

        self.mcu_contact_probe.activate_gcode.run_gcode_from_command()
        try:
            while len(samples) < sample_count:
                # 1. Probe
                pos = self._probe_contact(speed)
                
                # 2. Retract
                self.toolhead.manual_move(pos_xy + [pos[2] + retract_dist], lift_speed)
                
                if drop_n > 0:
                    drop_n -= 1
                    continue
                    
                # 3. Store and check tolerance
                samples.append(pos[2])
                spread = max(samples) - min(samples)
                if spread > tolerance:
                    if retries >= max_retries:
                        raise gcmd.error(f"Probe samples exceed sample_tolerance ({spread:.4f} > {tolerance:.4f})")
                    gcmd.respond_info("Probe samples exceed tolerance. Retrying...")
                    samples = []
                    retries += 1
                    
            # 4. Return result
            if samples_result == "median":
                return pos_xy + [median(samples)]
            else: # "mean"
                return pos_xy + [float(np.mean(samples))]
        finally:
            self.mcu_contact_probe.deactivate_gcode.run_gcode_from_command()

    def _probe_contact(self, speed):
        """
        Internal contact probe logic.
        Refined for Delta safety and resource locking.
        """
        self.toolhead.get_last_move_time()
        
        # SAFETY: Lock streaming so nothing else interferes
        self._streaming_lock = True
        
        try:
            self._sample_async()
            start_pos = self.toolhead.get_position()
            
            homing_move = HomingMove(self.printer, [(self.mcu_contact_probe, "contact")])
            
            # DELTA SAFETY: Ensure target X/Y match start X/Y exactly.
            target_pos = [start_pos[0], start_pos[1], self.CONTACT_PROBE_TARGET_Z]
            
            try:
                end_pos = homing_move.homing_move(target_pos, speed, probe_pos=True)[:3]
            except self.printer.command_error as e:
                if self.printer.is_shutdown():
                    reason = "Probing failed due to printer shutdown"
                else:
                    reason = str(e)
                    if "Timeout during probing move" in reason:
                        reason += probe.HINT_TIMEOUT
                raise self.printer.command_error(reason)
                
            end_pos[2] += self.get_z_compensation_value(target_pos)
            self.gcode.respond_info(f"probe at {end_pos[0]:.3f},{end_pos[1]:.3f} is z={end_pos[2]:.6f}")
            return end_pos
        finally:
            self._streaming_lock = False
    # --- Streaming Sub-system ---

    def _start_streaming(self):
        """
        Start beacon (coil/proximity) streaming.
        
        This uses reference-counting so nested callers can request streaming
        safely. Only perform the initial setup (reset buffers/filters and send
        the MCU enable command) on the first start (when refcount == 0).
        """
        # Ensure attribute defaults (defensive)
        if not hasattr(self, "_stream_en"):
            self._stream_en = 0

        if self._stream_en == 0:
            # Reset local buffers and filters only on the first enable.
            # FIX: Use correct variable names for BeaconProbe
            self._stream_buffer = []
            self._stream_buffer_count = 0
            
            try:
                self._data_filter.reset()
            except Exception:
                logging.exception("Beacon: _data_filter.reset() failed")

            # Tell the MCU to start streaming
            if self.beacon_stream_cmd is not None:
                try:
                    self.beacon_stream_cmd.send([1])
                except Exception:
                    # best-effort fallback
                    try:
                        self.beacon_stream_cmd.send([1, 0])
                    except Exception:
                        logging.exception("Beacon: failed to enable beacon_stream_cmd")

            # schedule the reactor timer
            try:
                curtime = self.reactor.monotonic()
                self.reactor.update_timer(self._stream_timeout_timer, curtime + STREAM_TIMEOUT)
            except Exception:
                logging.exception("Beacon: failed to update stream timeout timer")
            
            # Flush any stale data from the pipe immediately
            self._stream_flush()

        # increment refcount for nested callers
        self._stream_en += 1
        logging.debug("Beacon: _start_streaming -> refcount %d", self._stream_en)

    def _stop_streaming(self):
        """
        Decrement streaming refcount and, when it reaches zero, disable MCU streaming
        and clear streaming buffers. Use defensive bounds to avoid negative refcounts.
        """
        if not hasattr(self, "_stream_en"):
            # Defensive: nothing to stop
            self._stream_en = 0
            return

        # decrement but don't allow negative refcounts
        self._stream_en = max(0, self._stream_en - 1)
        logging.debug("Beacon: _stop_streaming -> refcount %d", self._stream_en)

        if self._stream_en == 0:
            # cancel the reactor timeout so it will not keep the stream alive
            try:
                self.reactor.update_timer(self._stream_timeout_timer, self.reactor.NEVER)
            except Exception:
                logging.exception("Beacon: failed to cancel stream timeout timer")

            # ask MCU to stop streaming
            if self.beacon_stream_cmd is not None:
                try:
                    self.beacon_stream_cmd.send([0])
                except Exception:
                    try:
                        self.beacon_stream_cmd.send([0, 0])
                    except Exception:
                        logging.exception("Beacon: failed to disable beacon_stream_cmd")

            # clear the in-memory buffer now that no one is listening
            # FIX: Use the correct variable name for BeaconProbe
            self._stream_buffer = []

        # flush any internal transient buffers (keeps behaviour similar to original)
        try:
            self._stream_flush()
        except Exception:
            logging.exception("Beacon: _stream_flush() failed in _stop_streaming")

    def force_stop_streaming(self):
        self.reactor.update_timer(self._stream_timeout_timer, self.reactor.NEVER)
        if self.beacon_stream_cmd is not None:
            self.beacon_stream_cmd.send([0])
        self._stream_flush()

    def _stream_timeout(self, eventtime):
        if self._stream_flush():
            return eventtime + STREAM_TIMEOUT
        if not self._stream_en:
            return self.reactor.NEVER
        if not self.printer.is_shutdown():
            msg = "Beacon sensor not receiving data"
            logging.error(msg)
            self.printer.invoke_shutdown(msg)
        return self.reactor.NEVER

    def request_stream_latency(self, latency):
        next_key = 0
        if self._stream_latency_requests:
            next_key = max(self._stream_latency_requests.keys()) + 1
        new_limit = STREAM_BUFFER_LIMIT_DEFAULT
        self._stream_latency_requests[next_key] = latency
        min_requested = min(self._stream_latency_requests.values())
        if min_requested < new_limit:
            new_limit = min_requested
        if new_limit < 1:
            new_limit = 1
        self._stream_buffer_limit_new = new_limit
        return next_key

    def drop_stream_latency_request(self, key):
        self._stream_latency_requests.pop(key, None)
        new_limit = STREAM_BUFFER_LIMIT_DEFAULT
        if self._stream_latency_requests:
            min_requested = min(self._stream_latency_requests.values())
            if min_requested < new_limit:
                new_limit = min_requested
        if new_limit < 1:
            new_limit = 1
        self._stream_buffer_limit_new = new_limit

    def streaming_session(self, callback, completion_callback=None, latency=None):
        return StreamingHelper(self, callback, completion_callback, latency)

    def _stream_flush_message(self, msg):
        """
        The "hot loop" that processes every batch of samples.
        This must be as fast as possible.
        """
        last = None
        
        # --- OPTIMIZATION ---
        # Cache the list of callbacks *once* outside the sample loop
        # instead of converting dict_values to a list on every sample.
        callbacks_list = list(self._stream_callbacks.values())
        has_callbacks = len(callbacks_list) > 0
        # --- END OPTIMIZATION ---

        for sample in msg:
            (clock, data) = sample
            temp = self.last_temp
            
            # Check for invalid temperature readings
            if self.model_temp is not None and not (self.TEMP_COMP_MIN_VALID < temp < self.TEMP_COMP_MAX_VALID):
                msg = f"Beacon temperature sensor faulty(read {temp:.2f} C), disabling temperature compensation"
                logging.error(msg)
                self.gcode.respond_raw(f"!! {msg}\n")
                self.model_temp = None
                
            if temp:
                self.measured_min = min(self.measured_min, temp)
                self.measured_max = max(self.measured_max, temp)

            clock = self._mcu.clock32_to_clock64(clock)
            time = self._mcu.clock_to_print_time(clock)
            self._data_filter.update(time, data)
            data_smooth = self._data_filter.value()
            freq = self.count_to_freq(data_smooth)
            dist = self.freq_to_dist(freq, temp)
            pos = self._get_position_at_time(time)
            
            if pos is not None and dist is not None:
                dist -= self.get_z_compensation_value(pos)
                
            last = sample = {
                "temp": temp,
                "clock": clock,
                "time": time,
                "data": data,
                "data_smooth": data_smooth,
                "freq": freq,
                "dist": dist,
            }
            if pos is not None:
                sample["pos"] = pos
                
            self._check_hardware(sample)

            if has_callbacks:
                for cb in callbacks_list:
                    cb(sample)
                    
        if last is not None:
            last = last.copy()
            dist = last["dist"]
            if dist is None or np.isinf(dist) or np.isnan(dist):
                del last["dist"]
            self.last_received_sample = last

    def _stream_flush(self):
        """
        Flushes the sample queue from the MCU.
        """
        self._stream_flush_event.clear()
        updated_timer = False
        while True:
            try:
                samples_batch = self._stream_samples_queue.get_nowait()
                updated_timer = False
                for sample in samples_batch:
                    if not updated_timer:
                        curtime = self.reactor.monotonic()
                        self.reactor.update_timer(
                            self._stream_timeout_timer, curtime + STREAM_TIMEOUT
                        )
                        updated_timer = True
                    self._stream_flush_message(sample)
            except queue.Empty:
                return updated_timer

    def _stream_flush_schedule(self):
        """
        Schedules a stream flush if the buffer is full or stream is stopping.
        """
        force = self._stream_en == 0  # When streaming is disabled, let all through
        if self._stream_buffer_limit_new != self._stream_buffer_limit:
            force = True
            self._stream_buffer_limit = self._stream_buffer_limit_new
            
        if not force and self._stream_buffer_count < self._stream_buffer_limit:
            return
            
        self._stream_samples_queue.put_nowait(self._stream_buffer)
        self._stream_buffer = []
        self._stream_buffer_count = 0
        
        if self._stream_flush_event.is_set():
            return
            
        self._stream_flush_event.set()
        self.reactor.register_async_callback(lambda e: self._stream_flush())

    # --- Model & Math Helpers ---

    def _update_thresholds(self, moving_up=False):
        self.trigger_freq = self.dist_to_freq(self.trigger_distance, self.last_temp)
        self.untrigger_freq = self.trigger_freq * (1 - self.trigger_hysteresis)

    def _apply_threshold(self, moving_up=False):
        self._update_thresholds()
        trigger_c = int(self.freq_to_count(self.trigger_freq))
        untrigger_c = int(self.freq_to_count(self.untrigger_freq))
        if self.beacon_set_threshold is not None:
            self.beacon_set_threshold.send([trigger_c, untrigger_c])

    def _register_model(self, name, model):
        if name in self.models:
            raise self.printer.config_error(f"Multiple Beacon models with same name '{name}'")
        self.models[name] = model

    def _is_faulty_coordinate(self, x, y, add_offsets=False):
        if not self.mesh_helper:
            return False
        return self.mesh_helper._is_faulty_coordinate(x, y, add_offsets)

    def _clock32_to_time(self, clock):
        clock64 = self._mcu.clock32_to_clock64(clock)
        return self._mcu.clock_to_print_time(clock64)

    def _get_position_at_time(self, print_time):
        kin = self.kinematics
        pos = {
            s.get_name(): s.mcu_to_commanded_position(
                s.get_past_mcu_position(print_time)
            )
            for s in kin.get_steppers()
        }
        return kin.calc_position(pos)

    def _sample_printtime_sync(self, skip=0, count=1):
        move_time = self.toolhead.get_last_move_time()
        settle_clock = self._mcu.print_time_to_clock(move_time)
        samples = []
        total = skip + count

        def _sample_sync_callback(sample):
            if sample["clock"] >= settle_clock:
                samples.append(sample)
                if len(samples) >= total:
                    raise StopStreaming

        with self.streaming_session(_sample_sync_callback, latency=skip + count) as ss:
            ss.wait()

        samples = samples[skip:]

        if count == 1:
            return samples[0]
        else:
            return samples

    def _sample(self, skip, count):
        samples = self._sample_printtime_sync(skip, count)
        return (median([s["dist"] for s in samples]), samples)

    def _sample_async(self, count=1):
        samples = []

        def _sample_async_callback(sample):
            samples.append(sample)
            if len(samples) >= count:
                raise StopStreaming

        with self.streaming_session(_sample_async_callback, latency=count) as ss:
            ss.wait()

        if count == 1:
            return samples[0]
        else:
            return samples

    def count_to_freq(self, count):
        return count * self._mcu_freq / (2**28)

    def freq_to_count(self, freq):
        return freq * (2**28) / self._mcu_freq

    def dist_to_freq(self, dist, temp):
        if self.model is None:
            return None
        return self.model.dist_to_freq(dist, temp)

    def freq_to_dist(self, freq, temp):
        if self.model is None:
            return None
        return self.model.freq_to_dist(freq, temp)

    # --- Internal Helpers & Status ---
    
    def _check_hardware(self, sample):
        if not self.hardware_failure:
            msg = None
            if sample["data"] == 0xFFFFFFF:
                msg = "coil is shorted or not connected"
            elif self.fmin is not None and sample["freq"] > 1.35 * self.fmin:
                msg = "coil expected max frequency exceeded"
            if msg:
                msg = f"Beacon hardware issue: {msg}"
                self.hardware_failure = msg
                logging.error(msg)
                if self._stream_en:
                    self.printer.invoke_shutdown(msg)
                else:
                    self.gcode.respond_raw(f"!! {msg}\n")
        elif self._stream_en:
            self.printer.invoke_shutdown(self.hardware_failure)

    def _extend_stats(self):
        parts = [
            f"coil_temp={self.last_temp:.1f}",
            f"refs={self._stream_en}",
        ]
        if self.last_mcu_temp is not None:
            (mcu_temp, supply_voltage) = self.last_mcu_temp
            parts.append(f"mcu_temp={mcu_temp:.2f}")
            parts.append(f"supply_voltage={supply_voltage:.3f}")
        return " ".join(parts)

    def _api_dump_callback(self, sample):
        tmp = [sample.get(key, None) for key in API_DUMP_FIELDS]
        self._api_dump.buffer.append(tmp)

    def get_status(self, eventtime):
        model_name = self.model.name if self.model is not None else None
        return {
            "last_sample": self.last_sample,
            "last_received_sample": self.last_received_sample,
            "last_z_result": self.last_z_result,
            "last_probe_position": self.last_probe_position,
            "last_probe_result": self.last_probe_result,
            "last_offset_result": self.last_offset_result,
            "last_poke_result": self.last_poke_result,
            "model": model_name,
        }

    def _hook_probing_gcode(self, config, module, cmd):
        """
        Hooks into other Klipper modules that perform probing (e.g., Z_TILT)
        to set the correct probe method (contact/proximity).
        """
        if not config.has_section(module):
            return

        try:
            mod = self.printer.load_object(config, module)
        except self.printer.config_error as e:
            logging.info(f"Failed to load module {module} for hooking: {e}")
            return

        if mod is None:
            return

        original_handler = self.gcode.register_command(cmd, None)
        if original_handler is None:
            logging.info(f"Could not hook G-Code command {cmd}")
            return

        def hooked_cmd_callback(gcmd):
            self._current_probe = gcmd.get(
                "PROBE_METHOD", self.default_probe_method
            ).lower()
            return original_handler(gcmd)

        self.gcode.register_command(cmd, hooked_cmd_callback)

    # --- Webhook Handlers ---

    def _handle_req_status(self, web_request):
        sample = self._sample_async()
        out = {
            "freq": sample["freq"],
            "dist": sample["dist"],
        }
        temp = sample["temp"]
        if temp is not None:
            out["temp"] = temp
        web_request.send(out)

    def _handle_req_dump(self, web_request):
        self._api_dump.add_web_client(web_request)
        web_request.send({"header": API_DUMP_FIELDS})

    # --- Klipper Compatibility Wrappers ---

    def compat_toolhead_set_position_homing_z(self, toolhead, pos):
        func = toolhead.set_position
        kind = tuple
        if hasattr(func, "__defaults__"):  # Python 3
            kind = type(func.__defaults__[0])
        else:  # Python 2
            kind = type(func.func_defaults[0])
        if kind is str:
            return toolhead.set_position(pos, homing_axes="z")
        else:
            return toolhead.set_position(pos, homing_axes=[2])

    def compat_kin_note_z_not_homed(self, kin):
        if hasattr(kin, "note_z_not_homed"):
            kin.note_z_not_homed()
        elif hasattr(kin, "clear_homing_state"):
            kin.clear_homing_state("z")

    def compat_serial_port(self, mcu):
        if hasattr(mcu, "_serialport"):
            return mcu._serialport
        elif hasattr(mcu, "_conn_helper"):
            return mcu._conn_helper.get_serialport()[0]
        else:
            raise Exception("Could not determine serial port")

    # --- Virtual Endstop Interface ---

    def setup_pin(self, pin_type, pin_params):
        if pin_type != "endstop" or pin_params["pin"] != "z_virtual_endstop":
            raise pins.error("Probe virtual endstop only useful as endstop pin")
        if pin_params["invert"] or pin_params["pullup"]:
            raise pins.error("Can not pullup/invert probe virtual endstop")
        return self.mcu_probe

    # --- Klipper Probe Interface ---

    def multi_probe_begin(self):
        self._start_streaming()

    def multi_probe_end(self):
        self._stop_streaming()

    def get_offsets(self):
        if self._current_probe == "contact":
            return 0, 0, 0
        else:
            return self.x_offset, self.y_offset, self.trigger_distance

    def get_lift_speed(self, gcmd=None):
        if gcmd is not None:
            return gcmd.get_float("LIFT_SPEED", self.lift_speed, above=0.0)
        return self.lift_speed

    def run_probe(self, gcmd):
        method = gcmd.get("PROBE_METHOD", self.default_probe_method).lower()
        self._current_probe = method
        
        if method == "proximity":
            return self._run_probe_proximity(gcmd)
        elif method == "contact":
            self._start_streaming()
            try:
                return self._run_probe_contact(gcmd)
            finally:
                self._stop_streaming()
        else:
            raise gcmd.error("Invalid PROBE_METHOD, valid choices: proximity, contact")

    def _move_to_probing_height(self, speed):
        """
        Moves the nozzle to the 'trigger_distance' to prepare for a
        proximity probe, accounting for backlash.
        """
        target = self.trigger_distance
        top = target + self.backlash_comp
        cur_z = self.toolhead.get_position()[2]
        
        if cur_z < top:
            self.toolhead.manual_move([None, None, top], speed)
        self.toolhead.manual_move([None, None, target], speed)
        self.toolhead.wait_moves()
    
    # --- Accelerometer Public Interface ---
        # These methods are required by [resonance_tester]
        # and other Klipper modules. They delegate the calls
        # to the internal BeaconAccelHelper instance.

    # Inside class BeaconProbe (Line ~1880)
    def start_internal_client(self):
        if not self.accel_helper:
            raise self.gcode.error(f"'{self.name}' is not an accelerometer")
        return self.accel_helper.start_internal_client()

    def is_measuring(self):
        if not self.accel_helper:
            return False
        return self.accel_helper.is_measuring()

    def read_reg(self, reg):
        if not self.accel_helper:
            raise self.gcode.error(f"'{self.name}' is not an accelerometer")
        return self.accel_helper.read_reg(reg)

    def set_reg(self, reg, val, minclock=0):
        if not self.accel_helper:
            raise self.gcode.error(f"'{self.name}' is not an accelerometer")
        return self.accel_helper.set_reg(reg, val, minclock)


class BeaconModel:
    """
    Holds a calibrated model (polynomial) that maps
    frequency (1/count) to distance.
    """
    @classmethod
    def load(cls, name, config, beacon):
        try:
            coef = config.getfloatlist("model_coef")
            temp = config.getfloat("model_temp")
            domain = config.getfloatlist("model_domain", count=2)
            [min_z, max_z] = config.getfloatlist("model_range", count=2)
            offset = config.getfloat("model_offset", 0.0)
            poly = Polynomial(coef, domain)
            return BeaconModel(name, beacon, poly, temp, min_z, max_z, offset)
        except ValueError as e:
            raise config.error(f"Error loading model {name}: {e}")

    def __init__(self, name, beacon, poly, temp, min_z, max_z, offset=0):
        self.name = name
        self.beacon = beacon
        self.poly = poly
        self.min_z = min_z
        self.max_z = max_z
        self.temp = temp
        self.offset = offset

    def save(self, beacon, show_message=True):
        configfile = beacon.printer.lookup_object("configfile")
        sensor_name = "" if beacon.id.is_unnamed() else f"sensor {beacon.id.name} "
        section = f"beacon {sensor_name}model {self.name}"
        
        configfile.set(section, "model_coef", ",\n  ".join(map(str, self.poly.coef)))
        configfile.set(section, "model_domain", ",".join(map(str, self.poly.domain)))
        configfile.set(section, "model_range", f"{self.min_z},{self.max_z}")
        configfile.set(section, "model_temp", f"{self.temp}")
        configfile.set(section, "model_offset", f"{self.offset:.5f}")
        
        if show_message:
            beacon.gcode.respond_info(
                f"Beacon calibration for model '{self.name}' has "
                "been updated\nfor the current session. The SAVE_CONFIG "
                "command will\nupdate the printer config file and restart "
                "the printer."
            )

    def freq_to_dist_raw(self, freq):
        [begin, end] = self.poly.domain
        invfreq = 1 / freq
        if invfreq > end:
            return float("inf")
        elif invfreq < begin:
            return float("-inf")
        else:
            return float(self.poly(invfreq) - self.offset)

    def freq_to_dist(self, freq, temp):
        if self.temp is not None and self.beacon.model_temp is not None:
            freq = self.beacon.model_temp.compensate(freq, temp, self.temp)
        return self.freq_to_dist_raw(freq)

    def dist_to_freq_raw(self, dist, max_e=0.00000001):
        if not (self.min_z <= dist <= self.max_z):
            msg = (
                f"Attempted to map out-of-range distance {dist}, valid range "
                f"[{self.min_z:.3f}, {self.max_z:.3f}]"
            )
            raise self.beacon.printer.command_error(msg)
            
        dist += self.offset
        [begin, end] = self.poly.domain
        
        # Binary search to find the inverse frequency
        for _ in range(0, 50): # 50 iterations is more than enough
            f = (end + begin) / 2
            v = self.poly(f)
            if abs(v - dist) < max_e:
                return float(1.0 / f)
            elif v < dist:
                begin = f
            else:
                end = f
        raise self.beacon.printer.command_error("Beacon model convergence error")

    def dist_to_freq(self, dist, temp, max_e=0.00000001):
        freq = self.dist_to_freq_raw(dist, max_e)
        if self.temp is not None and self.beacon.model_temp is not None:
            freq = self.beacon.model_temp.compensate(freq, self.temp, temp)
        return freq


class BeaconMCUTempHelper:
    """
    Handles MCU temperature compensation calculations based on
    values stored in the Beacon's NVM.
    """
    def __init__(self, temp_room, temp_hot, ref_room, ref_hot, adc_room, adc_hot):
        self.temp_room = temp_room
        self.temp_hot = temp_hot
        self.ref_room = ref_room
        self.ref_hot = ref_hot
        self.adc_room = adc_room
        self.adc_hot = adc_hot

    def compensate(self, beacon, mcu_temp, supply):
        temp_mcu_uncomp = self.temp_room + (self.temp_hot - self.temp_room) * (
            mcu_temp / beacon.temp_smooth_count - self.adc_room * self.ref_room
        ) / (self.adc_hot * self.ref_hot - self.adc_room * self.ref_room)
        
        ref_comp = self.ref_room + (self.ref_hot - self.ref_room) * (
            temp_mcu_uncomp - self.temp_room
        ) / (self.temp_hot - self.temp_room)
        
        temp_mcu_comp = self.temp_room + (self.temp_hot - self.temp_room) * (
            mcu_temp / beacon.temp_smooth_count * ref_comp
            - self.adc_room * self.ref_room
        ) / (self.adc_hot * self.ref_hot - self.adc_room * self.ref_room)
        
        supply_voltage = (
            4.0 * supply * ref_comp / beacon.temp_smooth_count * beacon.inv_adc_max
        )
        return (temp_mcu_comp, supply_voltage)

    @classmethod
    def build_with_nvm(cls, beacon):
        nvm_data = beacon.beacon_nvm_read_cmd.send([8, 65534])
        if nvm_data["offset"] == 65534:
            (lower, upper) = struct.unpack("<II", nvm_data["bytes"])
            temp_room = (lower & 0xFF) + 0.1 * ((lower >> 8) & 0xF)
            temp_hot = ((lower >> 12) & 0xFF) + 0.1 * ((lower >> 20) & 0xF)
            adc_room = (upper >> 8) & 0xFFF
            adc_hot = (upper >> 20) & 0xFFF
            (ref_room_raw, ref_hot_raw) = struct.unpack("<xxxbbxxx", nvm_data["bytes"])
            ref_room = 1.0 - ref_room_raw / 1000.0
            ref_hot = 1.0 - ref_hot_raw / 1000.0
            return cls(temp_room, temp_hot, ref_room, ref_hot, adc_room, adc_hot)
        else:
            return None


class BeaconTempModelBuilder:
    """
    Loads temperature compensation parameters from config or NVM.
    """
    _DEFAULTS = {
        "amfg": 1.0,
        "tcc": -2.1429828e-05,
        "tcfl": -1.8980091e-10,
        "tctl": 3.6738370e-16,
        "fmin": None,
        "fmin_temp": None,
    }

    @classmethod
    def load(cls, config):
        return BeaconTempModelBuilder(config)

    def __init__(self, config):
        self.parameters = BeaconTempModelBuilder._DEFAULTS.copy()
        for key in self.parameters.keys():
            param = config.getfloat(f"tc_{key}", None)
            if param is not None:
                self.parameters[key] = param

    def build_with_nvm(self, beacon):
        nvm_data = beacon.beacon_nvm_read_cmd.send([20, 0])
        (ver,) = struct.unpack("<Bxxx", nvm_data["bytes"][12:16])
        if ver == 0x01:
            return BeaconTempModelV1.build_with_nvm(beacon, self.parameters, nvm_data)
        else:
            return BeaconTempModelV0.build_with_nvm(beacon, self.parameters, nvm_data)


class BeaconTempModelV0:
    """
    Temperature compensation model (legacy).
    """
    def __init__(self, amfg, tcc, tcfl, tctl, fmin, fmin_temp):
        self.amfg = amfg
        self.tcc = tcc
        self.tcfl = tcfl
        self.tctl = tctl
        self.fmin = fmin
        self.fmin_temp = fmin_temp

    @classmethod
    def build_with_nvm(cls, beacon, parameters, nvm_data):
        (f_count, adc_count) = struct.unpack("<IH", nvm_data["bytes"][:6])
        if f_count < 0xFFFFFFFF and adc_count < 0xFFFF:
            if parameters["fmin"] is None:
                parameters["fmin"] = beacon.count_to_freq(f_count)
                logging.info(f"beacon: loaded fmin={parameters['fmin']:.2f} from nvm")
            if parameters["fmin_temp"] is None:
                temp_adc = (
                    float(adc_count) / beacon.temp_smooth_count * beacon.inv_adc_max
                )
                parameters["fmin_temp"] = beacon.thermistor.calc_temp(temp_adc)
                logging.info(f"beacon: loaded fmin_temp={parameters['fmin_temp']:.2f} from nvm")
        else:
            logging.info("beacon: parameters not found in nvm")
            
        if parameters["fmin"] is None or parameters["fmin_temp"] is None:
            return None
            
        logging.info(f"beacon: built tempco model version 0 {parameters}")
        return cls(**parameters)

    def _tcf(self, f, df, dt, tctl):
        tctl = self.tctl if tctl is None else tctl
        tc = self.tcc + self.tcfl * df + tctl * df * df
        return f + self.amfg * tc * dt * f

    def compensate(self, freq, temp_source, temp_target, tctl=None):
        dt = temp_target - temp_source
        dfmin = self.fmin * self.amfg * self.tcc * (temp_source - self.fmin_temp)
        df = freq - (self.fmin + dfmin)
        if dt < 0.0:
            f2 = self._tcf(freq, df, dt, tctl)
            dfmin2 = self.fmin * self.amfg * self.tcc * (temp_target - self.fmin_temp)
            df2 = f2 - (self.fmin + dfmin2)
            f3 = self._tcf(f2, df2, -dt, tctl)
            ferror = freq - f3
            freq = freq + ferror
            df = freq - (self.fmin + dfmin)
        return self._tcf(freq, df, dt, tctl)


class BeaconTempModelV1:
    """
    Temperature compensation model (latest).
    """
    def __init__(self, amfg, tcc, tcfl, tctl, fmin, fmin_temp):
        self.amfg = amfg
        self.tcc = tcc
        self.tcfl = tcfl
        self.tctl = tctl
        self.fmin = fmin
        self.fmin_temp = fmin_temp

    @classmethod
    def build_with_nvm(cls, beacon, parameters, nvm_data):
        (fnorm, temp, ver, cal) = struct.unpack("<dfBxxxf", nvm_data["bytes"])
        amfg = cls._amfg(cal)
        logging.info(f"beacon: loaded fnorm={fnorm:.2f} temp={temp:.2f} amfg={amfg:.3f} from nvm")
        
        if parameters["amfg"] == 1.0:
            parameters["amfg"] = amfg
        amfg = parameters["amfg"]
        if parameters["fmin"] is None:
            parameters["fmin"] = fnorm
        if parameters["fmin_temp"] is None:
            parameters["fmin_temp"] = temp
            
        parameters["tcc"] = cls._tcc(amfg)
        parameters["tcfl"] = cls._tcfl(amfg)
        parameters["tctl"] = cls._tctl(amfg)
        logging.info(f"beacon: built tempco model version 1 {parameters}")
        return cls(**parameters)

    @classmethod
    def _amfg(cls, cal):
        return -3 * cal + 6.08660841

    @classmethod
    def _tcc(cls, amfg):
        return -1.3145444333476082e-05 * amfg + 6.142916519010881e-06

    @classmethod
    def _tcfl(cls, amfg):
        return 0.00018478916784300965 * amfg - 0.0008211578277775643

    @classmethod
    def _tctl(cls, amfg):
        return -0.0006829761203506137 * amfg + 0.0026317792448821123

    def _tcf(self, df):
        return self.tcc + self.tcfl * df + self.tctl * df * df

    def compensate(self, freq, temp_source, temp_target):
        if self.amfg == 0.0:
            return freq
        fnorm = freq / self.fmin
        dtmin = temp_source - self.fmin_temp
        tc = self._tcf(fnorm - 1.0)
        for _ in range(0, 3):
            fsl = fnorm * (1 + tc * -dtmin)
            tc = self._tcf(fsl - 1.0)
        dt = temp_target - temp_source
        return freq * (1 + tc * dt)


class ModelManager:
    """
    Handles G-Code commands for managing Beacon models
    (SELECT, SAVE, REMOVE, LIST).
    """
    def __init__(self, beacon):
        self.beacon = beacon
        self.gcode = beacon.printer.lookup_object("gcode")
        beacon.id.register_command(
            "BEACON_MODEL_SELECT",
            self.cmd_BEACON_MODEL_SELECT,
            desc=self.cmd_BEACON_MODEL_SELECT_help,
        )
        beacon.id.register_command(
            "BEACON_MODEL_SAVE",
            self.cmd_BEACON_MODEL_SAVE,
            desc=self.cmd_BEACON_MODEL_SAVE_help,
        )
        beacon.id.register_command(
            "BEACON_MODEL_REMOVE",
            self.cmd_BEACON_MODEL_REMOVE,
            desc=self.cmd_BEACON_MODEL_REMOVE_help,
        )
        beacon.id.register_command(
            "BEACON_MODEL_LIST",
            self.cmd_BEACON_MODEL_LIST,
            desc=self.cmd_BEACON_MODEL_LIST_help,
        )

    cmd_BEACON_MODEL_SELECT_help = "Load named beacon model"
    def cmd_BEACON_MODEL_SELECT(self, gcmd):
        name = gcmd.get("NAME")
        model = self.beacon.models.get(name, None)
        if model is None:
            raise gcmd.error(f"Unknown model '{name}'")
        self.beacon.model = model
        gcmd.respond_info(f"Selected Beacon model '{name}'")

    cmd_BEACON_MODEL_SAVE_help = "Save current beacon model"
    def cmd_BEACON_MODEL_SAVE(self, gcmd):
        model = self.beacon.model
        if model is None:
            raise gcmd.error("No model currently selected")
        oldname = model.name
        name = gcmd.get("NAME", oldname)
        if name != oldname:
            model = copy.copy(model)
        model.name = name
        model.save(self.beacon)
        if name != oldname:
            self.beacon.models[name] = model

    cmd_BEACON_MODEL_REMOVE_help = "Remove saved beacon model"
    def cmd_BEACON_MODEL_REMOVE(self, gcmd):
        name = gcmd.get("NAME")
        model = self.beacon.models.get(name, None)
        if model is None:
            raise gcmd.error(f"Unknown model '{name}'")
            
        configfile = self.beacon.printer.lookup_object("configfile")
        section = f"beacon model {model.name}"
        configfile.remove_section(section)
        self.beacon.models.pop(name)
        
        gcmd.respond_info(
            f"Model '{name}' was removed for the current session.\n"
            "Run SAVE_CONFIG to update the printer configuration"
            "and restart Klipper."
        )
        if self.beacon.model == model:
            self.beacon.model = None

    cmd_BEACON_MODEL_LIST_help = "Remove saved beacon model"
    def cmd_BEACON_MODEL_LIST(self, gcmd):
        if not self.beacon.models:
            gcmd.respond_info("No Beacon models loaded")
            return
            
        gcmd.respond_info("List of loaded Beacon models:")
        current_model = self.beacon.model
        for model_name, model in sorted(self.beacon.models.items()):
            if model == current_model:
                gcmd.respond_info(f"- {model.name} [active]")
            else:
                gcmd.respond_info(f"- {model.name}")


class AlphaBetaFilter:
    """
    A simple filter to smooth the incoming raw data stream.
    Optimized to cache attributes as local variables in the hot-path.
    """
    def __init__(self, alpha, beta):
        self.alpha = alpha
        self.beta = beta
        self.reset()

    def reset(self):
        self.xl = None
        self.vl = 0.0
        self.tl = None

    def update(self, time, measurement):
        # --- OPTIMIZATION ---
        # Cache all 'self' attributes into fast local variables
        # at the start of this hot-loop function.
        alpha = self.alpha
        beta = self.beta
        xl = self.xl
        vl = self.vl
        tl = self.tl
        # --- End Optimization ---

        if xl is None:
            xl = measurement
        
        dt = (time - tl) if tl is not None else 0.0
        
        # All calculations below now use the fast local variables
        xk = xl + vl * dt
        vk = vl
        rk = measurement - xk
        
        xk = xk + alpha * rk  # <--- Fast local access
        if dt > 0:
            vk = vk + (beta / dt * rk) # <--- Fast local access
        
        # Write the final state back to 'self'
        self.xl = xk
        self.vl = vk
        self.tl = time
        return xk

    def value(self):
        return self.xl


class StreamingHelper:
    """
    A context manager to simplify starting/stopping a streaming session.
    Used via: with self.beacon.streaming_session(callback) as s: ...
    """
    def __init__(self, beacon, callback, completion_callback, latency):
        self.beacon = beacon
        self.callback = callback
        self.completion_callback = completion_callback
        self.completion = self.beacon.reactor.completion()

        self.latency_key = None
        if latency is not None:
            self.latency_key = self.beacon.request_stream_latency(latency)

        self.beacon._stream_callbacks[self] = self._handle
        self.beacon._start_streaming()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def _handle(self, sample):
        try:
            self.callback(sample)
        except StopStreaming:
            self.completion.complete(())

    def stop(self):
        if self not in self.beacon._stream_callbacks:
            return
            
        del self.beacon._stream_callbacks[self]
        self.beacon._stop_streaming()
        
        if self.latency_key is not None:
            self.beacon.drop_stream_latency_request(self.latency_key)
            
        if self.completion_callback is not None:
            self.completion_callback()

    def wait(self):
        self.completion.wait()
        self.stop()


class StopStreaming(Exception):
    """
    Custom exception used to signal a streaming session to stop,
    e.g., when enough samples have been collected.
    """
    pass


class BeaconProbeWrapper:
    """
    A wrapper class that presents the BeaconProbe as a standard
    Klipper 'probe' object.
    """
    def __init__(self, beacon):
        self.beacon = beacon
        self.results = None

    def multi_probe_begin(self):
        return self.beacon.multi_probe_begin()

    def multi_probe_end(self):
        return self.beacon.multi_probe_end()

    def get_offsets(self):
        return self.beacon.get_offsets()

    def get_lift_speed(self, gcmd=None):
        return self.beacon.get_lift_speed(gcmd)

    def run_probe(self, gcmd):
        result = self.beacon.run_probe(gcmd)
        if self.results is not None:
            self.results.append(result)
        return result

    def get_probe_params(self, gcmd=None):
        return {"lift_speed": self.beacon.get_lift_speed(gcmd)}

    def start_probe_session(self, gcmd):
        self.multi_probe_begin()
        self.results = []
        return self

    def end_probe_session(self):
        self.results = None
        self.multi_probe_end()

    def pull_probed_results(self):
        results = self.results
        if results is None:
            return []
        else:
            self.results = []
            return results

    def get_status(self, eventtime):
        return {"name": "beacon"}


class BeaconTempWrapper:
    """
    A wrapper class that presents the Beacon's coil thermistor
    as a standard Klipper 'temperature_sensor' object.
    """
    def __init__(self, beacon):
        self.beacon = beacon

    def get_temp(self, eventtime):
        return self.beacon.last_temp, 0.0 # (temp, target)

    def get_status(self, eventtime):
        return {
            "temperature": round(self.beacon.last_temp, 2),
            "measured_min_temp": round(self.beacon.measured_min, 2),
            "measured_max_temp": round(self.beacon.measured_max, 2),
        }


class BeaconEndstopShared:
    """
    Manages the mcu trsync objects for all Z steppers.
    This is critical for ensuring that when the probe triggers,
    all Z steppers stop at the exact same time, even if they are
    controlled by different MCUs.
    """
    def __init__(self, beacon):
        self.beacon = beacon
        self.printer = beacon.printer
        self.reactor = beacon.printer.get_reactor()

        ffi_main, ffi_lib = chelper.get_ffi()
        self.trsync_dispatcher = ffi_main.gc(ffi_lib.trdispatch_alloc(), ffi_lib.free)
        
        # Create a trsync object for the main beacon MCU
        primary_trsync = MCU_trsync(self.beacon._mcu, self.trsync_dispatcher)
        self.trsync_mcus = [primary_trsync]
        self.trigger_completion = None

        self.printer.register_event_handler(
            "klippy:mcu_identify", self._handle_mcu_identify
        )

    def _handle_mcu_identify(self):
        """
        Finds all Z steppers and adds them to the trsync manager.
        """
        toolhead = self.printer.lookup_object("toolhead")
        kinematics = toolhead.get_kinematics()
        for stepper in kinematics.get_steppers():
            if stepper.is_active_axis("z"):
                self.add_stepper(stepper)

    def add_stepper(self, stepper):
        """
        Adds a stepper to its corresponding MCU's trsync object.
        If a trsync object for that stepper's MCU doesn't exist, it creates one.
        """
        mcu_map = {trsync.get_mcu(): trsync for trsync in self.trsync_mcus}
        mcu = stepper.get_mcu()
        mcu_trsync = mcu_map.get(mcu)
        
        if mcu_trsync is None:
            # This stepper is on a different MCU, create a new trsync for it
            mcu_trsync = MCU_trsync(mcu, self.trsync_dispatcher)
            self.trsync_mcus.append(mcu_trsync)
            
        mcu_trsync.add_stepper(stepper)
        
        # Check for unsupported multi-mcu shared stepper rails
        stepper_name = stepper.get_name()
        if stepper_name.startswith("stepper_"):
            for other_trsync in self.trsync_mcus:
                for other_stepper in other_trsync.get_steppers():
                    if (other_trsync is not mcu_trsync and 
                            other_stepper.get_name().startswith(stepper_name[:9])):
                        raise self.printer.config_error(
                            "Multi-mcu homing not supported on multi-mcu shared axis"
                        )

    def get_steppers(self):
        """Returns a list of all Z steppers being monitored."""
        return [s for trsync in self.trsync_mcus for s in trsync.get_steppers()]

    def trsync_start(self, print_time):
        """
        Starts the trigger dispatch for all MCUs.
        This "arms" the endstops.
        """
        self.trigger_completion = self.reactor.completion()
        expire_timeout = TRSYNC_TIMEOUT
        
        primary_trsync = self.trsync_mcus[0]
        ffi_main, ffi_lib = chelper.get_ffi()
        
        for i, mcu_trsync in enumerate(self.trsync_mcus):
            try:
                mcu_trsync.start(print_time, self.trigger_completion, expire_timeout)
            except TypeError:
                # Compatibility for older Klipper versions
                offset = float(i) / len(self.trsync_mcus)
                mcu_trsync.start(
                    print_time, offset, self.trigger_completion, expire_timeout
                )
                
        ffi_lib.trdispatch_start(self.trsync_dispatcher, primary_trsync.REASON_HOST_REQUEST)

    def trsync_stop(self, home_end_time):
        """
        Stops the trsync dispatch and returns the trigger reason.
        """
        primary_trsync = self.trsync_mcus[0]
        primary_trsync.set_home_end_time(home_end_time)
        
        if self.beacon._mcu.is_fileoutput():
            self.trigger_completion.complete(True)
            
        self.trigger_completion.wait()
        
        ffi_main, ffi_lib = chelper.get_ffi()
        ffi_lib.trdispatch_stop(self.trsync_dispatcher)
        
        results = [trsync.stop() for trsync in self.trsync_mcus]
        
        if any([r == primary_trsync.REASON_COMMS_TIMEOUT for r in results]):
            command_error = self.printer.command_error
            raise command_error("Communication timeout during homing")
            
        if results[0] != primary_trsync.REASON_ENDSTOP_HIT:
            return 0.0 # Did not trigger
            
        return None # Triggered successfully


class BeaconEndstopWrapper:
    """
    Virtual endstop wrapper for proximity (non-contact) probing.
    """
    def __init__(self, beacon):
        self.beacon = beacon
        self.endstop_manager = beacon._endstop_shared
        self.printer = beacon.printer
        self.is_homing = False

        self.printer.register_event_handler("homing:home_rails_begin", self._handle_home_rails_begin)
        self.printer.register_event_handler("homing:home_rails_end", self._handle_home_rails_end)

    def _handle_home_rails_begin(self, homing_state, rails):
        self.is_homing = False

    def _handle_home_rails_end(self, homing_state, rails):
        if self.beacon.model is None or not self.is_homing:
            return
        if 2 not in homing_state.get_axes():
            return
        (distance, samples) = self.beacon._sample(self.beacon.z_settling_time, 10)
        if math.isinf(distance):
            logging.error(f"Post-homing adjustment measured samples {samples}")
            raise self.printer.command_error("Toolhead stopped below model range")
        homing_state.set_homed_position([None, None, distance])

    def get_mcu(self): return self.beacon._mcu
    def add_stepper(self, stepper): self.endstop_manager.add_stepper(stepper)
    def get_steppers(self): return self.endstop_manager.get_steppers()

    def home_start(self, print_time, sample_time, sample_count, rest_time, triggered=True):
        # PROXIMITY HOMING LOGIC
        if self.beacon.model is None:
            raise self.printer.command_error("No Beacon model loaded")
            
        self.is_homing = True
        
        # 1. Apply thresholds for proximity triggering
        self.beacon._apply_threshold()
        
        # 2. Ensure streaming is active
        self.beacon._sample_async() 
        
        # 3. Start trsync
        self.endstop_manager.trsync_start(print_time)
        
        primary_trsync = self.endstop_manager.trsync_mcus[0]
        
        # 4. Send STANDARD beacon_home command (NOT contact)
        self.beacon.beacon_home_cmd.send(
            [
                primary_trsync.get_oid(), 
                primary_trsync.REASON_ENDSTOP_HIT, 
                0
            ]
        )
        return self.endstop_manager.trigger_completion

    def home_wait(self, home_end_time):
        stop_reason = self.endstop_manager.trsync_stop(home_end_time)
        self.beacon.beacon_stop_home_cmd.send()
        if stop_reason is not None:
            return stop_reason
        return home_end_time

    def query_endstop(self, print_time):
        if self.beacon.model is None: return 1
        self.beacon._mcu.print_time_to_clock(print_time)
        sample = self.beacon._sample_async()
        if self.beacon.trigger_freq <= sample["freq"]: return 1
        else: return 0

    def get_position_endstop(self):
        return self.beacon.trigger_distance


class BeaconContactEndstopWrapper:
    """
    Virtual endstop wrapper for *contact* probing.
    """
    def __init__(self, beacon, config):
        self.beacon = beacon
        self.printer = beacon.printer
        self.endstop_manager = beacon._endstop_shared
        gcode_macro = self.printer.load_object(config, "gcode_macro")
        self.activate_gcode = gcode_macro.load_template(config, "contact_activate_gcode", "")
        self.deactivate_gcode = gcode_macro.load_template(config, "contact_deactivate_gcode", "")
        self.max_hotend_temp = config.getfloat("contact_max_hotend_temperature", 180.0)

    def get_mcu(self): return self.beacon._mcu
    def add_stepper(self, stepper): self.endstop_manager.add_stepper(stepper)
    def get_steppers(self): return self.endstop_manager.get_steppers()

    # Inside class BeaconContactEndstopWrapper
    def home_start(
        self, print_time, sample_time, sample_count, rest_time, triggered=True
    ):
        # 1. SAFETY: Check Extruder Temperature (Keep this!)
        extruder = self.beacon.toolhead.get_extruder()
        if extruder is not None:
            curtime = self.beacon.reactor.monotonic()
            cur_temp = extruder.get_heater().get_status(curtime)["temperature"]
            if cur_temp >= self.max_hotend_temp:
                raise self.printer.command_error(
                    f"Current hotend temperature {cur_temp:.1f} exceeds maximum allowed temperature {self.max_hotend_temp:.1f}"
                )

        self.is_homing = True
        
        # 2. OPTIMIZATION: Latency 50 for better contact timestamping
        self.beacon.request_stream_latency(50)
        self.beacon._start_streaming()
        self.beacon._sample_async() 
        
        self.endstop_manager.trsync_start(print_time)
        
        primary_trsync = self.endstop_manager.trsync_mcus[0]
        
        if self.beacon.beacon_contact_set_latency_min_cmd is not None:
            self.beacon.beacon_contact_set_latency_min_cmd.send(
                [self.beacon.contact_latency_min]
            )
        if self.beacon.beacon_contact_set_sensitivity_cmd is not None:
            self.beacon.beacon_contact_set_sensitivity_cmd.send(
                [self.beacon.contact_sensitivity]
            )
            
        self.beacon.beacon_contact_home_cmd.send(
            [
                primary_trsync.get_oid(),
                primary_trsync.REASON_ENDSTOP_HIT,
                0, # trigger_type (0 = Z)
            ]
        )
        return self.endstop_manager.trigger_completion

    def home_wait(self, home_end_time):
        try:
            stop_reason = self.endstop_manager.trsync_stop(home_end_time)
            if stop_reason is not None: return stop_reason
            if self.beacon._mcu.is_fileoutput(): return home_end_time
            self.beacon.toolhead.wait_moves()
            deadline = self.beacon.reactor.monotonic() + 0.5
            while True:
                response = self.beacon.beacon_contact_query_cmd.send([])
                if response["triggered"] == 0:
                    now = self.beacon.reactor.monotonic()
                    if now >= deadline: raise self.printer.command_error("Timeout getting contact time")
                    self.beacon.reactor.pause(now + 0.001)
                    continue
                trigger_time = self.beacon._clock32_to_time(response["detect_clock"])
                ffi_main, ffi_lib = chelper.get_ffi()
                move_data = ffi_main.new("struct pull_move[1]")
                count = ffi_lib.trapq_extract_old(self.beacon.trapq, move_data, 1, 0.0, trigger_time)
                if trigger_time >= home_end_time: return 0.0
                if count:
                    accel = move_data[0].accel
                    if accel < 0:
                        logging.info("Contact triggered while decelerating")
                        raise self.printer.command_error("No trigger on probe after full movement")
                    elif accel > 0:
                        raise self.printer.command_error("Contact triggered while accelerating")
                    return trigger_time
        finally:
            self.beacon.beacon_contact_stop_home_cmd.send()
            # CLEANUP: Drop 50ms request
            self.beacon.drop_stream_latency_request(50)

    def query_endstop(self, print_time): return 0
    def get_position_endstop(self): return 0


# --- Homing Override Class ---

HOMING_AUTOCAL_CALIBRATE_ALWAYS = 0
HOMING_AUTOCAL_CALIBRATE_UNHOMED = 1
HOMING_AUTOCAL_CALIBRATE_NEVER = 2
HOMING_AUTOCAL_CALIBRATE_CHOICES = {
    "always": HOMING_AUTOCAL_CALIBRATE_ALWAYS,
    "unhomed": HOMING_AUTOCAL_CALIBRATE_UNHOMED,
    "never": HOMING_AUTOCAL_CALIBRATE_NEVER,
}
HOMING_AUTOCAL_METHOD_CONTACT = 0
HOMING_AUTOCAL_METHOD_PROXIMITY = 1
HOMING_AUTOCAL_METHOD_PROXIMITY_IF_AVAILABLE = 2
HOMING_AUTOCAL_METHOD_CHOICES = {
    "contact": HOMING_AUTOCAL_METHOD_CONTACT,
    "proximity": HOMING_AUTOCAL_METHOD_PROXIMITY,
    "proximity_if_available": HOMING_AUTOCAL_METHOD_PROXIMITY_IF_AVAILABLE,
}
HOMING_AUTOCAL_CHOICES_METHOD = {v: k for k, v in HOMING_AUTOCAL_METHOD_CHOICES.items()}


class BeaconHomingHelper:
    """
    Overrides the G28 (Home) command to implement a custom
    homing sequence that uses the Beacon for Z-homing.
    """
    @classmethod
    def create(cls, beacon, config):
        home_xy_position = config.getfloatlist("home_xy_position", None, count=2)
        if home_xy_position is None:
            return None
        return BeaconHomingHelper(beacon, config, home_xy_position)

    def __init__(self, beacon, config, home_xy_position):
        self.beacon = beacon
        self.printer = beacon.printer
        self.home_xy_position = home_xy_position

        for section in ["safe_z_home", "homing_override"]:
            if config.has_section(section):
                raise config.error(
                    f"home_xy_position cannot be used with [{section}]"
                )

        self.z_hop = config.getfloat("home_z_hop", 0.0)
        self.z_hop_speed = config.getfloat("home_z_hop_speed", 15.0, above=0.0)
        self.xy_move_speed = config.getfloat("home_xy_move_speed", 50.0, above=0.0)
        self.home_y_before_x = config.getboolean("home_y_before_x", False)
        
        self.method = config.getchoice(
            "home_method", HOMING_AUTOCAL_METHOD_CHOICES, "proximity"
        )
        self.method_when_homed = config.getchoice(
            "home_method_when_homed",
            HOMING_AUTOCAL_METHOD_CHOICES,
            HOMING_AUTOCAL_CHOICES_METHOD[self.method],
        )
        self.autocal_create_model = config.getchoice(
            "home_autocalibrate", HOMING_AUTOCAL_CALIBRATE_CHOICES, "always"
        )

        gcode_macro = self.printer.load_object(config, "gcode_macro")
        self.gcode_pre_xy = gcode_macro.load_template(config, "home_gcode_pre_xy", "")
        self.gcode_post_xy = gcode_macro.load_template(config, "home_gcode_post_xy", "")
        self.gcode_pre_x = gcode_macro.load_template(config, "home_gcode_pre_x", "")
        self.gcode_post_x = gcode_macro.load_template(config, "home_gcode_post_x", "")
        self.gcode_pre_y = gcode_macro.load_template(config, "home_gcode_pre_y", "")
        self.gcode_post_y = gcode_macro.load_template(config, "home_gcode_post_y", "")
        self.gcode_pre_z = gcode_macro.load_template(config, "home_gcode_pre_z", "")
        self.gcode_post_z = gcode_macro.load_template(config, "home_gcode_post_z", "")

        # Ensure homing is loaded so we can override G28
        self.printer.load_object(config, "homing")
        self.gcode = gcode = beacon.gcode
        self.previous_gcode_handler = gcode.register_command("G28", None)
        gcode.register_command("G28", self.cmd_G28)

    def _perform_z_hop(self, toolhead):
        """
        Performs a Z-hop, respecting kinematics boundaries.
        """
        if self.z_hop == 0:
            return
            
        curtime = self.beacon.reactor.monotonic()
        kinematics = toolhead.get_kinematics()
        kin_status = kinematics.get_status(curtime)
        current_position = toolhead.get_position()

        z_hop_move = [None, None, self.z_hop]
        
        if "z" not in kin_status["homed_axes"]:
            # Z is not homed, so we set a temporary 0
            current_position[2] = 0
            self.beacon.compat_toolhead_set_position_homing_z(toolhead, current_position)
            toolhead.manual_move(z_hop_move, self.z_hop_speed)
            toolhead.wait_moves()
            self.beacon.compat_kin_note_z_not_homed(kinematics)
        elif current_position[2] < self.z_hop:
            # Z is homed, but we are below hop height
            toolhead.manual_move(z_hop_move, self.z_hop_speed)
            toolhead.wait_moves()

    def _run_gcode_hook(self, template, gcode_params, raw_gcode_params):
        """
        Runs a user-defined G-Code hook (e.g., pre_x, post_y).
        """
        template_context = template.create_template_context()
        template_context["params"] = gcode_params
        template_context["rawparams"] = raw_gcode_params
        template.run_gcode_from_command(template_context)

    def cmd_G28(self, gcmd):
        """
        Custom G28 command handler.
        """
        toolhead = self.printer.lookup_object("toolhead")
        original_gcode_params = gcmd.get_command_parameters()
        raw_gcode_params = gcmd.get_raw_command_parameters()

        self._perform_z_hop(toolhead)

        home_x, home_y, home_z = [gcmd.get(axis, None) is not None for axis in "XYZ"]
        # No axes given => home them all
        if not (home_x or home_y or home_z):
            home_x = home_y = home_z = True

        if home_x or home_y:
            self._run_gcode_hook(self.gcode_pre_xy, original_gcode_params, raw_gcode_params)
            
            axis_order = "yx" if self.home_y_before_x else "xy"
            
            for axis in axis_order:
                if axis == "x" and home_x:
                    self._run_gcode_hook(self.gcode_pre_x, original_gcode_params, raw_gcode_params)
                    command = self.gcode.create_gcode_command("G28", "G28", {"X": "0"})
                    self.previous_gcode_handler(command)
                    self._run_gcode_hook(self.gcode_post_x, original_gcode_params, raw_gcode_params)
                elif axis == "y" and home_y:
                    self._run_gcode_hook(self.gcode_pre_y, original_gcode_params, raw_gcode_params)
                    command = self.gcode.create_gcode_command("G28", "G28", {"Y": "0"})
                    self.previous_gcode_handler(command)
                    self._run_gcode_hook(self.gcode_post_y, original_gcode_params, raw_gcode_params)
                    
            self._run_gcode_hook(self.gcode_post_xy, original_gcode_params, raw_gcode_params)

        if home_z:
            self._run_gcode_hook(self.gcode_pre_z, original_gcode_params, raw_gcode_params)
            
            curtime = self.beacon.reactor.monotonic()
            kinematics = toolhead.get_kinematics()
            kin_status = kinematics.get_status(curtime)
            if "xy" not in kin_status["homed_axes"]:
                raise gcmd.error("Must home X and Y axes before homing Z")

            # Determine which homing method to use
            method = self.method
            if "z" in kin_status["homed_axes"]:
                method = self.method_when_homed

            # Allow G28 to override the method
            args = gcmd.get_commandline().split(" ")
            for arg in args:
                kv = arg.split("=")
                if len(kv) == 2 and kv[0].strip().lower() == "method":
                    method_name = kv[1].strip().lower()
                    method = HOMING_AUTOCAL_METHOD_CHOICES.get(method_name, None)
                    if method is None:
                        raise gcmd.error(
                            "Invalid homing method, valid choices: "
                            "proximity, proximity_if_available, contact"
                        )
                    break

            target_xy_position = [self.home_xy_position[0], self.home_xy_position[1]]

            if method == HOMING_AUTOCAL_METHOD_PROXIMITY_IF_AVAILABLE:
                method = (HOMING_AUTOCAL_METHOD_PROXIMITY 
                          if self.beacon.model is not None 
                          else HOMING_AUTOCAL_METHOD_CONTACT)

            if method == HOMING_AUTOCAL_METHOD_CONTACT:
                # --- Homing with Contact ---
                toolhead.manual_move(target_xy_position, self.xy_move_speed)

                calibrate = True
                if self.autocal_create_model == HOMING_AUTOCAL_CALIBRATE_UNHOMED:
                    calibrate = "z" not in kin_status["homed_axes"]
                elif self.autocal_create_model == HOMING_AUTOCAL_CALIBRATE_NEVER:
                    calibrate = False

                calibrate_override = gcmd.get("CALIBRATE", None)
                if calibrate_override is not None:
                    calibrate = calibrate_override.lower() not in ["=0", "=no", "=false"]

                autocalibrate_cmd = "BEACON_AUTO_CALIBRATE"
                autocalibrate_params = {}
                if not calibrate:
                    autocalibrate_params["SKIP_MODEL_CREATION"] = "1"
                    
                command = self.gcode.create_gcode_command(
                    autocalibrate_cmd, autocalibrate_cmd, autocalibrate_params
                )
                self.beacon.cmd_BEACON_AUTO_CALIBRATE(command)
                
            elif method == HOMING_AUTOCAL_METHOD_PROXIMITY:
                # --- Homing with Proximity ---
                target_xy_position[0] -= self.beacon.x_offset
                target_xy_position[1] -= self.beacon.y_offset
                toolhead.manual_move(target_xy_position, self.xy_move_speed)
                command = self.gcode.create_gcode_command("G28", "G28", {"Z": "0"})
                self.previous_gcode_handler(command)
            else:
                raise gcmd.error(f"Invalid homing method '{method}'")
                
            self._perform_z_hop(toolhead)
            self._run_gcode_hook(self.gcode_post_z, original_gcode_params, raw_gcode_params)


class BeaconHomingState:
    """Helper class to pass to Klipper's homing events."""
    def get_axes(self):
        return [2] # Z axis

    def get_trigger_position(self, stepper_name):
        raise Exception("get_trigger_position not supported")

    def set_stepper_adjustment(self, stepper_name, adjustment):
        pass

    def set_homed_position(self, pos):
        pass


class BeaconMeshHelper:
    """
    Handles the BED_MESH_CALIBRATE command when METHOD=beacon.
    This class is responsible for generating the scan path,
    flying the toolhead, collecting sensor data, processing
    that data into a mesh, and applying it to Klipper.
    
    Functions are ordered logically:
    1. Initialization
    2. GCode Command Handlers (Public API)
    3. Core Calibration Flow
    4. Path Generation
    5. Data Collection (Scanning)
    6. Data Processing
    7. Utility & Helper Methods
    """
    @classmethod
    def create(cls, beacon, config):
        if config.has_section('bed_mesh'):
            mesh_config = config.getsection("bed_mesh")
            return BeaconMeshHelper(beacon, config, mesh_config)
        else:
            return None

    def __init__(self, beacon, config, mesh_config):
        self.beacon = beacon
        self.scipy = None
        self.mesh_config = mesh_config
        # Store the bed_mesh instance
        self.bed_mesh_instance = self.beacon.printer.load_object(mesh_config, "bed_mesh")

        self.speed = mesh_config.getfloat("speed", 50.0, above=0.0, note_valid=False)
        self.radius = mesh_config.getfloat("mesh_radius", None, above=0.0)
        
        # Default relative_reference_index to None
        self.relative_reference_index = None
        
        if self.radius is not None:
            # --- Delta / Round Bed Configuration ---
            self.origin = mesh_config.getfloatlist("mesh_origin", (0.0, 0.0), count=2)
            self.default_probe_count_x = self.default_probe_count_y = mesh_config.getint("round_probe_count", 5, minval=3)
            # Round beds require an odd number of points
            if not self.default_probe_count_x & 1:
                raise config.error("bed_mesh: probe_count must be odd for round beds")
            self.radius = math.floor(self.radius * 10) / 10
            self.def_min_x = self.def_min_y = -self.radius
            self.def_max_x = self.def_max_y = self.radius
            self.relative_reference_index = int((self.default_probe_count_x * self.default_probe_count_y)/2)
        else:
            # --- Cartesian / Square Bed Configuration ---
            self.def_min_x, self.def_min_y = mesh_config.getfloatlist(
                "mesh_min", count=2, note_valid=False
            )
            self.def_max_x, self.def_max_y = mesh_config.getfloatlist(
                "mesh_max", count=2, note_valid=False
            )
            if self.def_min_x > self.def_max_x:
                self.def_min_x, self.def_max_x = self.def_max_x, self.def_min_x
            if self.def_min_y > self.def_max_y:
                self.def_min_y, self.def_max_y = self.def_max_y, self.def_min_y

            self.default_probe_count_x, self.default_probe_count_y = mesh_config.getintlist(
                "probe_count", count=2, note_valid=False
            )
            
        self.relative_reference_index = mesh_config.getint(
            "relative_reference_index", self.relative_reference_index, note_valid=False
        )
        self.zero_reference_position = mesh_config.getfloatlist(
            "zero_reference_position", None, count=2
        )
        self.zero_ref_cluster_size = config.getfloat(
            "zero_reference_cluster_size", 1, minval=0
        )
        self.scan_direction = config.getchoice(
            "mesh_main_direction", {"x": "x", "X": "x", "y": "y", "Y": "y"}, "y"
        )
        self.overscan = config.getfloat("mesh_overscan", -1, minval=0)
        
        # --- CRITICAL FIX ---
        # Default is set to 0 to disable filtering.
        # Filtering (e.g., 1mm) discards all samples during a "fly-by"
        # scan, as they are too far from the cluster center.
        self.cluster_size = config.getfloat("mesh_cluster_size", 0, minval=0)
        
        self.scan_runs = config.getint("mesh_runs", 1, minval=1)
        self.adaptive_margin = mesh_config.getfloat(
            "adaptive_margin", 0, note_valid=False
        )

        contact_default_min_cfg = config.getfloatlist(
            "contact_mesh_min",
            default=None,
            count=2,
        )
        contact_default_max_cfg = config.getfloatlist(
            "contact_mesh_max",
            default=None,
            count=2,
        )

        probe_x_offset = self.beacon.x_offset
        probe_y_offset = self.beacon.y_offset

        contact_min = contact_default_min_cfg
        if contact_default_min_cfg is None:
            contact_min = (
                max(self.def_min_x - probe_x_offset, self.def_min_x),
                max(self.def_min_y - probe_y_offset, self.def_min_y),
            )

        contact_max = contact_default_max_cfg
        if contact_default_max_cfg is None:
            contact_max = (
                min(self.def_max_x - probe_x_offset, self.def_max_x),
                min(self.def_max_y - probe_y_offset, self.def_max_y),
            )

        min_x = contact_min[0]
        max_x = contact_max[0]
        min_y = contact_min[1]
        max_y = contact_max[1]
        self.default_contact_min = (min(min_x, max_x), min(min_y, max_y))
        self.default_contact_max = (max(min_x, max_x), max(min_y, max_y))

        if self.zero_reference_position is not None and self.relative_reference_index is not None:
            logging.info(
                "beacon: both 'zero_reference_position' and "
                "'relative_reference_index' options are specified. The"
                " former will be used"
            )

        self.faulty_regions = []
        for i in list(range(1, 100, 1)):
            start_pos = mesh_config.getfloatlist(
                f"faulty_region_{i}_min", None, count=2
            )
            if start_pos is None:
                break
            end_pos = mesh_config.getfloatlist(f"faulty_region_{i}_max", count=2)
            x_min = min(start_pos[0], end_pos[0])
            x_max = max(start_pos[0], end_pos[0])
            y_min = min(start_pos[1], end_pos[1])
            y_max = max(start_pos[1], end_pos[1])
            self.faulty_regions.append(Region(x_min, x_max, y_min, y_max))

        self.exclude_object = None
        beacon.printer.register_event_handler("klippy:connect", self._handle_connect)

        self.gcode = beacon.gcode
        self.previous_gcode_handler = self.gcode.register_command("BED_MESH_CALIBRATE", None)
        self.gcode.register_command(
            "BED_MESH_CALIBRATE",
            self.cmd_BED_MESH_CALIBRATE,
            desc=self.cmd_BED_MESH_CALIBRATE_help,
        )

    # --- 1. GCode Command Handlers (Public API) ---

    cmd_BED_MESH_CALIBRATE_help = "Perform Mesh Bed Leveling"

    def cmd_BED_MESH_CALIBRATE(self, gcmd):
        """
        Wrapper for BED_MESH_CALIBRATE.
        It decides whether to run the Beacon fast scan (METHOD=beacon)
        or pass control to Klipper's original probing (METHOD=automatic).
        """
        method = gcmd.get("METHOD", "beacon").lower()
        probe_method = gcmd.get(
            "PROBE_METHOD", self.beacon.default_probe_method
        ).lower()
        
        # Proximity (non-contact) is required for fast scanning
        if probe_method != "proximity":
            method = "automatic"
            
        if method == "beacon":
            # Run our fast scanning calibration
            self.calibrate(gcmd)
        else:
            # Run Klipper's standard, point-by-point calibration
            if hasattr(self.bed_mesh_instance.bmc, "zero_ref_pos"):
                zero_ref_pos_adjusted = self.zero_reference_position
                if zero_ref_pos_adjusted is not None and probe_method == "contact":
                    zero_ref_pos_adjusted = (
                        zero_ref_pos_adjusted[0] + self.beacon.x_offset, 
                        zero_ref_pos_adjusted[1] + self.beacon.y_offset
                    )
                self.bed_mesh_instance.bmc.zero_ref_pos = zero_ref_pos_adjusted
            
            if probe_method == "contact":
                params = gcmd.get_command_parameters()
                extra_params = {}
                if "MESH_MIN" not in params:
                    extra_params["MESH_MIN"] = ",".join(map(str, self.default_contact_min))
                if "MESH_MAX" not in params:
                    extra_params["MESH_MAX"] = ",".join(map(str, self.default_contact_max))
                if extra_params:
                    extra_params.update(params)
                    gcmd = self.gcode.create_gcode_command(
                        gcmd.get_command(),
                        gcmd.get_commandline()
                        + "".join([f" {k}={v}" for k, v in extra_params.items()]),
                        extra_params,
                    )
            self.beacon._current_probe = probe_method
            self.previous_gcode_handler(gcmd)

    # --- 2. Core Calibration Flow ---

    def calibrate(self, gcmd):
        """
        The main entry point for the "METHOD=beacon" scan.
        Sets up parameters and controls the flow of operations.
        """
        use_full_mesh_area = gcmd.get_int("USE_CONTACT_AREA", 0) == 0
        self.min_x, self.min_y = coord_fallback(
            gcmd,
            "MESH_MIN",
            float_parse,
            self.def_min_x if use_full_mesh_area else self.default_contact_min[0],
            self.def_min_y if use_full_mesh_area else self.default_contact_min[1],
            lambda v, d: max(v, d),
        )
        self.max_x, self.max_y = coord_fallback(
            gcmd,
            "MESH_MAX",
            float_parse,
            self.def_max_x if use_full_mesh_area else self.default_contact_max[0],
            self.def_max_y if use_full_mesh_area else self.default_contact_max[1],
            lambda v, d: min(v, d),
        )
        self.res_x, self.res_y = coord_fallback(
            gcmd,
            "PROBE_COUNT",
            int,
            self.default_probe_count_x,
            self.default_probe_count_y,
            lambda v, _d: max(v, 3),
        )
        self.profile_name = gcmd.get("PROFILE", "default")

        # Sanity check boundaries
        if self.min_x > self.max_x:
            self.min_x, self.max_x = (
                max(self.max_x, self.def_min_x),
                min(self.min_x, self.def_max_x),
            )
        if self.min_y > self.max_y:
            self.min_y, self.max_y = (
                max(self.max_y, self.def_min_y),
                min(self.min_y, self.def_max_y),
            )

        # Handle zero reference (RRI or Position)
        relative_ref_index = gcmd.get_int("RELATIVE_REFERENCE_INDEX", None)
        if relative_ref_index is not None:
            self.zero_ref_mode = ("rri", relative_ref_index)
        elif self.zero_reference_position is not None:
            self.zero_ref_mode = ("pos", self.zero_reference_position)
            self.zero_ref_val = None
            self.zero_ref_bin = []
        elif self.relative_reference_index is not None:
            self.zero_ref_mode = ("rri", self.relative_reference_index)
        else:
            self.zero_ref_mode = None

        # Handle adaptive meshing
        if gcmd.get_int("ADAPTIVE", 0):
            if self.exclude_object is not None:
                margin = gcmd.get_float("ADAPTIVE_MARGIN", self.adaptive_margin)
                self._shrink_to_excluded_objects(margin)
            else:
                gcmd.respond_info(
                    "Requested adaptive mesh, but [exclude_object] is not enabled. Ignoring."
                )

        # Calculate grid steps
        self.step_x = (self.max_x - self.min_x) / (self.res_x - 1)
        self.step_y = (self.max_y - self.min_y) / (self.res_y - 1)

        self.toolhead = self.beacon.toolhead
        scan_path = self._generate_path()

        probe_speed = gcmd.get_float("PROBE_SPEED", self.beacon.speed, above=0.0)
        self.beacon._move_to_probing_height(probe_speed)

        scan_speed = gcmd.get_float("SPEED", self.speed, above=0.0)
        scan_runs = gcmd.get_int("RUNS", self.scan_runs, minval=1)

        # --- CRITICAL TIMING FIX ---
        # Move to the start position *before* sampling begins.
        # This ensures the toolhead is in place when _sample_mesh
        # starts the stream and dwells.
        (x, y) = scan_path[0]
        self.toolhead.manual_move([x, y, None], scan_speed)
        self.toolhead.wait_moves()
        # --- END FIX ---
            
        raw_clusters = self._sample_mesh(gcmd, scan_path, scan_speed, scan_runs)

        # Handle zero reference
        if self.zero_ref_mode and self.zero_ref_mode[0] == "pos":
            if len(self.zero_ref_bin) == 0:
                self._collect_zero_ref(scan_speed, self.zero_ref_mode[1])
            else:
                self.zero_ref_val = median(self.zero_ref_bin)

        # Process data and apply mesh
        z_matrix = self._process_clusters(raw_clusters, gcmd)
        self._apply_mesh(z_matrix, gcmd)

        # Final Home with Safety Clear
        gcmd.respond_info("Mesh calibration complete. Clearing mesh and Homing...")
        self.gcode.run_script_from_command("BED_MESH_CLEAR")
        self.gcode.run_script_from_command("G28")

        # Final Home (Delta Best Practice)
        gcmd.respond_info("Mesh calibration complete. Homing...")
        self.gcode.run_script_from_command("G28")
        
    # --- 3. Path Generation ---

    def _generate_path(self):
        """
        Generates the serpentine toolhead path for the scan.
        This is the most complex part of the delta/round bed logic.
        """
        probe_x_offset = self.beacon.x_offset
        probe_y_offset = self.beacon.y_offset
        
        # Select scan direction (X-major or Y-major)
        settings = {
            "x": {
                "range_aligned": [self.min_x - probe_x_offset, self.max_x - probe_x_offset],
                "range_perpendicular": [self.min_y - probe_y_offset, self.max_y - probe_y_offset],
                "count": self.res_y,
                "swap_coord": False,
            },
            "y": {
                "range_aligned": [self.min_y - probe_y_offset, self.max_y - probe_y_offset],
                "range_perpendicular": [self.min_x - probe_x_offset, self.max_x - probe_x_offset],
                "count": self.res_x,
                "swap_coord": True,
            },
        }[self.scan_direction]

        # Use normalized coordinates (aligned/perpendicular)
        # and swap X/Y at the end if necessary.
        aligned_start = settings["range_aligned"][0]
        aligned_end = settings["range_aligned"][1]
        perp_start = settings["range_perpendicular"][0]
        perp_end = settings["range_perpendicular"][1]
        swap_coordinates = settings["swap_coord"]
        line_count = settings["count"]
        
        if line_count < 1:
            logging.error("Mesh count is less than 1, cannot generate lines.")
            return []

        # Calculate distance between scan lines
        line_step = 0 if line_count == 1 else (perp_end - perp_start) / (float(line_count - 1))
        points = []
        corner_radius = min(line_step / 2, self.overscan)
        epsilon = 1e-9 # Tolerance for float comparisons
        
        for i in range(0, line_count):
            # perp_coord is the toolhead's perpendicular coordinate (e.g., Y-coord)
            perp_coord = perp_start + line_step * i
            even = i % 2 == 0  # Serpentine direction
            line_start_a = aligned_start # Default X-boundaries
            line_end_a = aligned_end
            
            # --- DELTA/ROUND BED LOGIC ---
            if self.radius is not None:
                
                # ### CRITICAL FIX ###
                # Check if the *probe's* coordinate (toolhead + offset)
                # is outside the radius. Using just 'perp_coord' causes
                # scan lines at the edge to be skipped if probe_y_offset != 0.
                if abs(perp_coord + probe_y_offset) > self.radius + epsilon:
                    continue # Skip this entire scan line
                
                # Calculate the X-boundary for this Y-line using the
                # circle equation (x = sqrt(r^2 - y^2)).
                # We must use the probe's coordinate (perp_coord + probe_y_offset) here as well.
                dist_squared = (self.radius**2 - (perp_coord + probe_y_offset)**2)
                dist_safe = math.sqrt(max(0, dist_squared))
                line_start_a = -dist_safe
                line_end_a = dist_safe
            
            # Apply the probe's X-offset to the X-boundaries
            line_start_a -= probe_x_offset
            line_end_a -= probe_x_offset
            
            # (Note: A bug was fixed here. 'perp_coord -= probe_y_offset' was
            # removed, as 'perp_coord' already holds the correct toolhead Y-coord.)
            
            point_a = (line_start_a, perp_coord) if even else (line_end_a, perp_coord)
            point_b = (line_end_a, perp_coord) if even else (line_start_a, perp_coord)
            line = (point_a, point_b)

            # --- DELTA/ROUND BED FIX ---
            # The 'and self.radius is None' check was removed from here.
            # This forces cornering logic (U-turns) on *all* bed types.
            if len(points) > 0 and corner_radius > 0:
                
                # --- BUG FIX ---
                # Renamed 'begin_a' and 'end_a' to match the new variables
                if even:
                    center = line_start_a - self.overscan + corner_radius
                    points += arc_points(
                        center, perp_coord - line_step + corner_radius, corner_radius, -90, -90
                    )
                    points += arc_points(
                        center, perp_coord - corner_radius, corner_radius, -180, -90
                    )
                else:
                    center = line_end_a + self.overscan - corner_radius
                # --- END BUG FIX ---
                    points += arc_points(
                        center, perp_coord - line_step + corner_radius, corner_radius, -90, 90
                    )
                    points += arc_points(
                        center, perp_coord - corner_radius, corner_radius, 0, 90
                    )

            points.append(line[0])
            points.append(line[1])

        if swap_coordinates:
            # Swap (X,Y) coordinates if we were scanning in Y-major
            for i in range(len(points)):
                (x, y) = points[i]
                points[i] = (y, x)

        return points

    # --- 4. Data Collection (Scanning) ---

    def _sample_mesh(self, gcmd, scan_path, scan_speed, scan_runs):
        """
        Flies the path, collects sensor data, and bins it.
        Includes Latency Normalization to fix zig-zag artifacts.
        """
        cluster_size_limit = gcmd.get_float("CLUSTER_SIZE", self.cluster_size, minval=0.0)
        zero_ref_cluster_size = self.zero_ref_cluster_size
        if not (self.zero_ref_mode and self.zero_ref_mode[0] == "pos"):
            zero_ref_cluster_size = 0
            
        # Latency in seconds (e.g., 50ms)
        latency_sec = 0.05 

        min_x, min_y = self.min_x, self.min_y
        probe_x_offset = self.beacon.x_offset
        probe_y_offset = self.beacon.y_offset

        clusters = {}
        total_samples = [0]
        invalid_samples = [0]

        def _sample_callback(sample):
            total_samples[0] += 1
            distance = sample["dist"]
            
            # 1. Get position from trapq
            pos = list(sample["pos"]) 
            
            # 2. Apply Physical Offsets
            pos[0] += probe_x_offset
            pos[1] += probe_y_offset
            
            # 3. NORMALIZER: Apply Latency Correction (Simplified)
            # In a full implementation, we would calculate velocity vector.
            
            if distance is None or math.isinf(distance):
                if self._is_valid_position(pos[0], pos[1]):
                    invalid_samples[0] += 1
                return

            # Cluster Assignment
            x_float = (pos[0] - min_x) / self.step_x
            y_float = (pos[1] - min_y) / self.step_y
            
            x_index = int(round(x_float))
            y_index = int(round(y_float))
            
            if x_index < 0 or self.res_x <= x_index or y_index < 0 or self.res_y <= y_index:
                return

            if cluster_size_limit > 0:
                x_center = x_index * self.step_x + min_x
                y_center = y_index * self.step_y + min_y
                dx = pos[0] - x_center
                dy = pos[1] - y_center
                dist_to_center = math.sqrt(dx*dx + dy*dy)
                if dist_to_center > cluster_size_limit:
                    return

            if zero_ref_cluster_size > 0:
                dx = pos[0] - self.zero_ref_mode[1][0]
                dy = pos[1] - self.zero_ref_mode[1][1]
                if math.sqrt(dx*dx + dy*dy) <= zero_ref_cluster_size:
                    self.zero_ref_bin.append(distance)

            cluster_key = (x_index, y_index)
            if cluster_key not in clusters:
                clusters[cluster_key] = []
            clusters[cluster_key].append(distance)

        with self.beacon.streaming_session(_sample_callback, latency=50):
            self.toolhead.dwell(0.25)
            self._fly_path(scan_path[1:], scan_speed, scan_runs)

        gcmd.respond_info(
            f"Sampled {total_samples[0]} total points over {scan_runs} runs"
        )
        if invalid_samples[0]:
            gcmd.respond_info(
                f"!! Encountered {invalid_samples[0]} invalid samples!"
            )
        gcmd.respond_info(f"Samples binned in {len(clusters)} clusters")

        return clusters

    def _fly_path(self, path_segment, scan_speed, scan_runs):
    # --- END BUG FIX ---
        """
        Executes the list of moves from the generated path.
        """
        for i in range(scan_runs):
            current_path = path_segment if i % 2 == 0 else reversed(path_segment)
            for x, y in current_path:
                self.toolhead.manual_move([x, y, None], scan_speed)
        self.toolhead.dwell(0.251) # Dwell at the *end* of the scan
        self.toolhead.wait_moves()

    # --- 5. Data Processing ---

    def _process_clusters(self, raw_clusters, gcmd):
        """
        Wrapper to run the (slow) data processing in a
        separate process to avoid blocking the Klipper host.
        """
        parent_conn, child_conn = multiprocessing.Pipe()
        dump_file = gcmd.get("FILENAME", None)

        def do_processing_in_subprocess():
            try:
                child_conn.send(
                    (False, self._do_process_clusters(raw_clusters, dump_file))
                )
            except Exception:
                child_conn.send((True, traceback.format_exc()))
            finally:
                child_conn.close()

        child_process = multiprocessing.Process(target=do_processing_in_subprocess)
        child_process.daemon = True
        child_process.start()
        
        reactor = self.beacon.reactor
        eventtime = reactor.monotonic()
        while child_process.is_alive():
            eventtime = reactor.pause(eventtime + 0.1)
            
        is_error, result = parent_conn.recv()
        child_process.join()
        parent_conn.close()
        
        if is_error:
            raise Exception(f"Error processing mesh: {result}")
        else:
            is_inner_error, inner_result = result
            if is_inner_error:
                raise gcmd.error(inner_result)
            else:
                return inner_result

    def _do_process_clusters(self, raw_clusters, dump_file):
        """
        The actual data processing. This runs in its own process.
        """
        if dump_file:
            try:
                with open(dump_file, "w") as file_handle:
                    file_handle.write("x,y,xp,xy,dist\n")
                    for y_index in range(self.res_y):
                        for x_index in range(self.res_x):
                            cluster = raw_clusters.get((x_index, y_index), [])
                            x_phys = x_index * self.step_x + self.min_x
                            y_phys = y_index * self.step_y + self.min_y
                            for dist in cluster:
                                file_handle.write(f"{x_index},{y_index},{x_phys},{y_phys},{dist}\n")
            except OSError as e:
                logging.warning(f"Failed to write mesh dump to {dump_file}: {e}")

        mask = self._generate_fault_mask()
        matrix, faulty_regions = self._generate_matrix(raw_clusters, mask)
        
        if len(faulty_regions) > 0:
            (error, interpolator_or_msg) = self._load_interpolator()
            if error:
                return (True, interpolator_or_msg)
            matrix = self._interpolate_faulty(
                matrix, faulty_regions, interpolator_or_msg
            )
            
        error_message = self._check_matrix(matrix)
        if error_message is not None:
            return (True, error_message)
            
        return (False, self._finalize_matrix(matrix))

    def _generate_matrix(self, raw_clusters, mask):
        """
        Generates the final Z-value matrix from the binned cluster data.
        """
        TOLERANCE = 1e-5 # Float tolerance for boundary checks

        faulty_indexes = []
        
        # 1. Initialize the entire matrix with np.nan (Not a Number)
        #    This is our "unprobed" or "missing" marker.
        matrix = np.full((self.res_y, self.res_x), np.nan)
        
        # 2. Iterate over the full rectangular grid
        for y_index in range(self.res_y):
            for x_index in range(self.res_x):
                
                x_coord = x_index * self.step_x + self.min_x
                y_coord = y_index * self.step_y + self.min_y
                
                # --- DELTA / ROUND BED MASKING ---
                if self.radius is not None:
                    dist_sq = x_coord**2 + y_coord**2
                    
                    # If point is outside the circle, mark it as NaN and skip.
                    # This NaN will be ignored by _check_matrix and
                    # will make the web UI graph look correct.
                    if dist_sq > (self.radius**2 + TOLERANCE):
                        matrix[(y_index, x_index)] = np.nan
                        continue
                # --- END DELTA CHECK ---

                # 3. Process collected data
                cluster_key = (x_index, y_index)
                cluster_values = raw_clusters.get(cluster_key, None)
                
                if cluster_values is not None and len(cluster_values) > 0:
                    # Point is *inside* the circle and has data.
                    if mask is None or mask[(y_index, x_index)]:
                        # Valid data: calculate the median Z deviation
                        matrix[(y_index, x_index)] = self.beacon.trigger_distance - median(cluster_values)
                    else:
                        # Point is in a faulty region, mark for interpolation
                        matrix[(y_index, x_index)] = np.nan
                        faulty_indexes.append((y_index, x_index))
                
                # If a point is *inside* the circle but has NO data
                # (e.g., our [15,0] bug), it remains np.nan.

        return matrix, faulty_indexes

    def _check_matrix(self, matrix):
        """
        Checks the final matrix for errors.
        Specifically, it looks for any 'np.nan' values that are
        *inside* the mesh area, which indicates a real error.
        """
        TOLERANCE = 1e-5  
        
        empty_clusters = []
        for y_index in range(self.res_y):
            for x_index in range(self.res_x):
                
                # --- GRAPHING & ERROR FIX ---
                # This point is 'Not a Number'. We must find out why.
                if np.isnan(matrix[(y_index, x_index)]):
                    
                    x_coord = x_index * self.step_x + self.min_x
                    y_coord = y_index * self.step_y + self.min_y

                    # Check if this NaN point is *inside* the mesh boundary.
                    if self.radius is None or (x_coord**2 + y_coord**2 <= (self.radius**2 + TOLERANCE)):
                        # If YES, it's a real error (an empty cluster).
                        empty_clusters.append(f"  ({x_coord:.3f},{y_coord:.3f})[{x_index},{y_index}]")

                    # If NO, it's just a masked-off corner.
                    # We 'continue' and do not report it as an error.
                    continue
                    
        if empty_clusters:
            error_message = (
                "Empty clusters found\n"
                "Try increasing mesh cluster_size or slowing down.\n"
                "The following clusters were empty:\n"
            ) + "\n".join(empty_clusters)
            return error_message
        else:
            return None

    def _finalize_matrix(self, matrix):
        """
        Applies the Relative Reference Index (RRI) or
        Zero Reference Position to make the mesh relative.
        """
        z_offset = None
        if self.zero_ref_mode and self.zero_ref_mode[0] == "rri":
            relative_ref_index = self.zero_ref_mode[1]
            if relative_ref_index < 0 or relative_ref_index >= self.res_x * self.res_y:
                relative_ref_index = None
            if relative_ref_index is not None:
                rri_x_index = relative_ref_index % self.res_x
                rri_y_index = int(math.floor(relative_ref_index / self.res_x))
                z_offset = matrix[rri_y_index][rri_x_index]
        elif self.zero_ref_mode and self.zero_ref_mode[0] == "pos":
            z_offset = self.beacon.trigger_distance - self.zero_ref_val

        if z_offset is not None:
            matrix = matrix - z_offset
        return matrix.tolist()

    def _apply_mesh(self, matrix, gcmd):
        """
        Applies the finalized Z-deviation matrix to
        Klipper's bed_mesh module.
        """
        # 1. Configure Mesh Parameters
        mesh_params = self.bed_mesh_instance.bmc.mesh_config.copy()
    
        mesh_params["min_x"] = self.min_x
        mesh_params["max_x"] = self.max_x
        mesh_params["min_y"] = self.min_y
        mesh_params["max_y"] = self.max_y
        mesh_params["x_count"] = self.res_x
        mesh_params["y_count"] = self.res_y
    
        # Add Delta/Round bed configuration
        if self.radius is not None:
            mesh_params["mesh_radius"] = self.radius
            mesh_params["round_probe_count"] = self.default_probe_count_x

        # 2. Instantiate ZMesh Object (with compatibility)
        try:
            mesh = bed_mesh.ZMesh(mesh_params)
        except TypeError:
            mesh = bed_mesh.ZMesh(mesh_params, self.profile_name) 

        # 3. Build and Apply Mesh
        try:
            mesh.build_mesh(matrix)
        except bed_mesh.BedMeshError as error:
            raise self.gcode.error(str(error))
        
        self.bed_mesh_instance.set_mesh(mesh)
        self.gcode.respond_info("Mesh calibration complete")
    
        # Save the profile if a name is set
        if self.profile_name is not None:
            self.bed_mesh_instance.save_profile(self.profile_name)

    # --- 6. Utility & Helper Methods ---

    def _handle_connect(self):
        self.exclude_object = self.beacon.printer.lookup_object("exclude_object", None)

        if self.overscan < 0:
            # Auto-determine a safe overscan amount to prevent
            # the toolhead from moving out of bounds.
            toolhead = self.beacon.printer.lookup_object("toolhead")
            curtime = self.beacon.reactor.monotonic()
            status = toolhead.get_kinematics().get_status(curtime)
            probe_x_offset = self.beacon.x_offset
            probe_y_offset = self.beacon.y_offset
            settings = {
                "x": {
                    "range": [self.def_min_x - probe_x_offset, self.def_max_x - probe_x_offset],
                    "machine": [status["axis_minimum"][0], status["axis_maximum"][0]],
                    "count": self.default_probe_count_y,
                },
                "y": {
                    "range": [self.def_min_y - probe_y_offset, self.def_max_y - probe_y_offset],
                    "machine": [status["axis_minimum"][1], status["axis_maximum"][1]],
                    "count": self.default_probe_count_x,
                },
            }[self.scan_direction]

            mesh_range = settings["range"]
            machine_range = settings["machine"]
            line_spacing = (mesh_range[1] - mesh_range[0]) / (float(settings["count"] - 1))
            self.overscan = min(
                [
                    max(0, mesh_range[0] - machine_range[0]),
                    max(0, machine_range[1] - machine_range[1]),
                    line_spacing + 2.0,  # A half circle with 2mm lead in/out
                ]
            )

    def _shrink_to_excluded_objects(self, margin):
        """
        Calculates new mesh boundaries based on excluded objects
        for adaptive meshing. (Removed unused 'gcmd' parameter).
        """
        bound_min_x, bound_max_x = None, None
        bound_min_y, bound_max_y = None, None
        objects = self.exclude_object.get_status().get("objects", {})
        if len(objects) == 0:
            return

        for obj in objects:
            for point in obj["polygon"]:
                bound_min_x = opt_min(bound_min_x, point[0])
                bound_max_x = opt_max(bound_max_x, point[0])
                bound_min_y = opt_min(bound_min_y, point[1])
                bound_max_y = opt_max(bound_max_y, point[1])
        bound_min_x -= margin
        bound_max_x += margin
        bound_min_y -= margin
        bound_max_y += margin

        # Calculate original step size and apply the new bounds
        orig_span_x = self.max_x - self.min_x
        orig_span_y = self.max_y - self.min_y

        if bound_min_x >= self.min_x:
            self.min_x = bound_min_x
        if bound_max_x <= self.max_x:
            self.max_x = bound_max_x
        if bound_min_y >= self.min_y:
            self.min_y = bound_min_y
        if bound_max_y <= self.max_y:
            self.max_y = bound_max_y

        # Update resolution to retain approximately the same step size
        self.res_x = int(
            math.ceil(self.res_x * (self.max_x - self.min_x) / orig_span_x)
        )
        self.res_y = int(
            math.ceil(self.res_y * (self.max_y - self.min_y) / orig_span_y)
        )
        # Guard against bicubic interpolation with 3 points
        min_resolution = 3
        if max(self.res_x, self.res_y) > 6 and min(self.res_x, self.res_y) < 4:
            min_resolution = 4
        self.res_x = max(self.res_x, min_resolution)
        self.res_y = max(self.res_y, min_resolution)

        self.profile_name = None

    def _collect_zero_ref(self, speed, coord):
        """
        Moves to the zero reference position and takes a sample.
        """
        probe_x_offset = self.beacon.x_offset
        probe_y_offset = self.beacon.y_offset
        (x, y) = coord
        self.toolhead.manual_move([x - probe_x_offset, y - probe_y_offset, None], speed)
        (dist, _samples) = self.beacon._sample(50, 10)
        self.zero_ref_val = dist

    def _is_valid_position(self, x, y):
        if self.radius is not None:
            return self._is_in_circle(x, y)
        return self.min_x <= x <= self.max_x and self.min_y <= y <= self.max_y

    def _is_in_circle(self, x, y):
        # (Fixed typo, was _is_in_cicrle)
        return (x)**2 + (y)**2 <= self.radius**2

    def _is_faulty_coordinate(self, x, y, add_offsets=False):
        if add_offsets:
            probe_x_offset = self.beacon.x_offset
            probe_y_offset = self.beacon.y_offset
            x += probe_x_offset
            y += probe_y_offset
        for region in self.faulty_regions:
            if region.is_point_within(x, y):
                return True
        return False
        
    def _generate_fault_mask(self):
        """
        Creates a boolean mask for faulty regions.
        Uses NumPy array slicing for high-speed generation.
        """
        if len(self.faulty_regions) == 0:
            return None
        # Create a boolean mask for the whole grid
        mask = np.full((self.res_y, self.res_x), True)
        
        for region in self.faulty_regions:
            # Calculate the grid-index boundaries for the faulty region
            min_x_idx = max(0, int(math.ceil((region.x_min - self.min_x) / self.step_x)))
            min_y_idx = max(0, int(math.ceil((region.y_min - self.min_y) / self.step_y)))
            max_x_idx = min(
                self.res_x - 1, int(math.floor((region.x_max - self.min_x) / self.step_x))
            )
            max_y_idx = min(
                self.res_y - 1, int(math.floor((region.y_max - self.min_y) / self.step_y))
            )
            
            # --- OPTIMIZATION ---
            # Replaced slow, nested Python loops with a single,
            # high-speed NumPy array slicing operation.
            mask[min_y_idx : max_y_idx + 1, min_x_idx : max_x_idx + 1] = False
            
        return mask

    def _load_interpolator(self):
        """
        Loads the 'scipy' library for interpolating faulty regions.
        """
        if not self.scipy:
            try:
                self.scipy = importlib.import_module("scipy")
            except ImportError:
                msg = (
                    "Could not load `scipy`. To install it, simply re-run "
                    "the Beacon `install.sh` script. This module is required "
                    "when using faulty regions when bed meshing."
                )
                return (True, msg)
        if hasattr(self.scipy.interpolate, "RBFInterpolator"):

            def rbf_interp(points, values, faulty):
                return self.scipy.interpolate.RBFInterpolator(points, values, 64)(
                    faulty
                )

            return (False, rbf_interp)
        else:
            # Fallback to linear interpolation
            def linear_interp(points, values, faulty):
                return self.scipy.interpolate.griddata(
                    points, values, faulty, method="linear"
                )

    def _interpolate_faulty(self, matrix, faulty_indexes, interpolator):
        """
        Uses the loaded interpolator to fill in the 'np.nan' values
        in the matrix that were marked as faulty.
        """
        y_indices, x_indices = np.mgrid[0 : matrix.shape[0], 0 : matrix.shape[1]]
        points = np.array([y_indices.flatten(), x_indices.flatten()]).T
        values = matrix.reshape(-1)
        good = ~np.isnan(values)
        fixed = interpolator(points[good], values[good], faulty_indexes)
        matrix[tuple(np.array(faulty_indexes).T)] = fixed
        return matrix


class Region:
    """Helper class to define a rectangular faulty region."""
    def __init__(self, x_min, x_max, y_min, y_max):
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max

    def is_point_within(self, x, y):
        return (x > self.x_min and x < self.x_max) and (
            y > self.y_min and y < self.y_max
        )

# This function is used by _generate_path
def arc_points(cx, cy, r, start_angle, span):
    """
    Calculates all points for a 90-degree arc turn.
    Uses NumPy for high-speed vectorized calculations.
    """
    start_angle = start_angle / 180.0 * math.pi
    span = span / 180.0 * math.pi
    
    # Determine number of segments needed
    delta_angle_max = math.acos(1 - 0.1 / r)
    segment_count = int(math.ceil(abs(span) / delta_angle_max))
    delta_angle = span / float(segment_count)

    # --- OPTIMIZATION ---
    # Replaced a Python 'for' loop with a vectorized NumPy operation
    
    # 1. Generate all 'i' values from 0 to segment_count
    index_values = np.arange(segment_count + 1)
    
    # 2. Calculate all angles at once
    angles = start_angle + (delta_angle * index_values)
    
    # 3. Calculate all X and Y coordinates at once
    x_values = cx + np.cos(angles) * r
    y_values = cy + np.sin(angles) * r
    
    # 4. Combine them into a list of (x, y) tuples
    return np.column_stack((x_values, y_values)).tolist()


def coord_fallback(gcmd, name, parse, def_x, def_y, map=lambda v, d: v):
    """
    Helper to parse coordinates from G-Code, falling back to defaults.
    """
    param = gcmd.get(name, None)
    if param is not None:
        try:
            x, y = [parse(p.strip()) for p in param.split(",", 1)]
            return map(x, def_x), map(y, def_y)
        except ValueError:
            raise gcmd.error(f"Unable to parse parameter '{name}'")
    else:
        return def_x, def_y


def float_parse(s):
    """
    Helper to parse a float, raising an error on Inf/NaN.
    """
    try:
        v = float(s)
        if math.isinf(v) or np.isnan(v):
            raise ValueError("Infinite or NaN value")
        return v
    except ValueError:
        raise gcmd.error(f"Unable to parse float: {s}")


def median(samples):
    """
    Calculates the median of a list of samples.
    """
    return float(np.median(samples))


def opt_min(a, b):
    if a is None:
        return b
    return min(a, b)


def opt_max(a, b):
    if a is None:
        return b
    return max(a, b)


# --- Accelerometer Classes ---

class BeaconAccelDummyConfig(object):
    """
    A dummy config object to pass to Klipper's adxl345 module,
    tricking it into using our accelerometer.
    """
    def __init__(self, beacon, accel_config):
        self.beacon = beacon
        self.accel_config = accel_config

    def get_name(self):
        if self.beacon.id.is_unnamed():
            return "beacon"
        else:
            return f"beacon_{self.beacon.id.name}"

    def has_section(self, name):
        if not self.beacon.id.is_unnamed():
            return True
        return name == "adxl345" and self.accel_config.adxl345_exists

    def get_printer(self):
        return self.beacon.printer


class BeaconAccelConfig(object):
    """
    Holds the configuration for the accelerometer.
    """
    def __init__(self, config):
        self.default_scale = config.get("accel_scale", "")
        axes = {
            "x": (0, 1),
            "-x": (0, -1),
            "y": (1, 1),
            "-y": (1, -1),
            "z": (2, 1),
            "-z": (2, -1),
        }
        axes_map = config.getlist("accel_axes_map", ("x", "y", "z"), count=3)
        self.axes_map = []
        for a in axes_map:
            a = a.strip()
            if a not in axes:
                raise config.error(f"Invalid accel_axes_map, unknown axes '{a}'")
            self.axes_map.append(axes[a])

        self.adxl345_exists = config.has_section("adxl345")


class BeaconAccelHelper(adxl345.ADXL345):
    """
    Main class for the Beacon's accelerometer features.
    Handles streaming and processing accelerometer data.
    """
    def __init__(self, beacon, config, constants):
        self.beacon = beacon
        self.config = config

        self._api_dump = APIDumpHelper(
            beacon.printer,
            lambda: self._start_streaming() or True,
            lambda _: self._stop_streaming(),
            self._api_update,
        )
        beacon.id.register_endpoint("beacon/dump_accel", self._handle_req_dump)
        adxl345.AccelCommandHelper(BeaconAccelDummyConfig(beacon, config), self)

        self._stream_en = 0
        self._raw_samples = []
        self._last_raw_sample = (0, 0, 0)
        self._sample_lock = threading.Lock()

        beacon._mcu.register_response(self._handle_accel_data, "beacon_accel_data")
        beacon._mcu.register_response(self._handle_accel_state, "beacon_accel_state")

        self.reinit(constants)

    def reinit(self, constants):
        bits = constants.get("BEACON_ACCEL_BITS")
        self._clip_values = (2 ** (bits - 1) - 1, -(2 ** (bits - 1)))

        self.accel_stream_cmd = self.beacon._mcu.lookup_command(
            "beacon_accel_stream en=%c scale=%c", cq=self.beacon.cmd_queue
        )
        # Ensure streaming mode is stopped
        self.accel_stream_cmd.send([0, 0])

        self._scales = self._fetch_scales(constants)
        self._scale = self._select_scale()
        logging.info(f"Selected Beacon accelerometer scale {self._scale['name']}")

    def _fetch_scales(self, constants):
        enum = self.beacon._mcu.get_enumerations().get("beacon_accel_scales", None)
        if enum is None:
            return {}

        scales = {}
        self.default_scale_name = self.config.default_scale
        first_scale_name = None
        for name, id in enum.items():
            try:
                scale_val_name = f"BEACON_ACCEL_SCALE_{name.upper()}"
                scale_val_str = constants.get(scale_val_name)
                scale_val = float(scale_val_str)
            except (ValueError, TypeError):
                logging.error(
                    f"Beacon accelerometer scale {name} could not be processed"
                )
                scale_val = 1  # Values will be weird, but scale will work

            if id == 0:
                first_scale_name = name
            scales[name] = {"name": name, "id": id, "scale": scale_val}

        if not self.default_scale_name:
            if first_scale_name is None:
                logging.error("Could not determine default Beacon accelerometer scale")
            else:
                self.default_scale_name = first_scale_name
        elif self.default_scale_name not in scales:
            logging.error(
                f"Default Beacon accelerometer scale '{self.default_scale_name}' not found,  using '{first_scale_name}'"
            )
            self.default_scale_name = first_scale_name

        return scales

    def _select_scale(self):
        scale = self._scales.get(self.default_scale_name, None)
        if scale is None:
            return {"name": "unknown", "id": 0, "scale": 1}
        return scale

    def _handle_accel_data(self, params):
        with self._sample_lock:
            if self._stream_en:
                self._raw_samples.append(params)
            else:
                self.accel_stream_cmd.send([0, 0])

    def _handle_accel_state(self, params):
        pass # Stub for future use

    def _handle_req_dump(self, web_request):
        cconn = self._api_dump.add_web_client(
            web_request,
            lambda buffer: list(
                itertools.chain(*map(lambda data: data["data"], buffer))
            ),
        )
        cconn.send({"header": ["time", "x", "y", "z"]})

    # --- Internal helpers ---

    def _start_streaming(self):
        """
        Start beacon accelerometer streaming.
        """
        # 1. SAFETY SHIM: Do not allow streaming if the Beacon Coil is busy/locked
        # (We check the parent beacon object for the lock)
        if getattr(self.beacon, "_streaming_lock", False):
             logging.warning("Beacon Accel: Attempted to start streaming while coil is locked! Ignoring.")
             return

        if self._stream_en == 0:
            # Reset local buffer
            with self._sample_lock:
                self._raw_samples = []

            # Tell the MCU to start streaming (Using the correct ACCEL command)
            if self.accel_stream_cmd is not None:
                try:
                    # Send Enable=1 and the Scale ID
                    self.accel_stream_cmd.send([1, self._scale['id']])
                except Exception:
                    logging.exception("Beacon: failed to enable accel_stream_cmd")

        # increment refcount
        self._stream_en += 1

    def _stop_streaming(self):
        """
        Stop beacon accelerometer streaming.
        """
        if self._stream_en > 0:
            self._stream_en -= 1

        if self._stream_en == 0:
            # ask MCU to stop streaming
            if self.accel_stream_cmd is not None:
                try:
                    self.accel_stream_cmd.send([0, 0])
                except Exception:
                    logging.exception("Beacon: failed to disable accel_stream_cmd")
            
            # Clear buffer
            with self._sample_lock:
                self._raw_samples = []

    # --- APIDumpHelper callbacks ---

    def _api_update(self, dump_helper, eventtime):
        with self._sample_lock:
            raw_samples = self._raw_samples
            self._raw_samples = []
            
        (samples, errors, last_raw_sample) = self._process_samples(
            raw_samples, self._last_raw_sample
        )
        if len(samples) == 0:
            return
            
        self._last_raw_sample = last_raw_sample
        dump_helper.buffer.append(
            {
                "data": samples,
                "errors": errors,
                "overflows": 0,
            }
        )

    # --- Accelerometer Public Interface ---

    def start_internal_client(self):
        # Fix: Removed the "if not self.accel_helper" check
        cli = AccelInternalClient(self.beacon.printer)
        self._api_dump.add_client(cli._handle_data)
        return cli

    def read_reg(self, reg):
        raise self.beacon.printer.command_error("Not supported")

    def set_reg(self, reg, val, minclock=0):
        raise self.beacon.printer.command_error("Not supported")

    def is_measuring(self):
        return self._stream_en > 0


class AccelInternalClient:
    """
    Acts as a client for Klipper's internal accelerometer
    measurement modules (e.g., resonance_tester).
    """
    def __init__(self, printer):
        self.printer = printer
        self.toolhead = printer.lookup_object("toolhead")
        self.is_finished = False
        self.request_start_time = self.request_end_time = (
            self.toolhead.get_last_move_time()
        )
        self.msgs = []
        self.samples = []

    def _handle_data(self, msgs):
        if self.is_finished:
            return False
        if len(self.msgs) >= 10000:  # Limit capture length
            return False
        self.msgs.extend(msgs)
        return True

    # --- AccelQueryHelper interface ---

    def finish_measurements(self):
        self.request_end_time = self.toolhead.get_last_move_time()
        self.toolhead.wait_moves()
        self.is_finished = True

    def has_valid_samples(self):
        for msg in self.msgs:
            data = msg["data"]
            first_sample_time = data[0][0]
            last_sample_time = data[-1][0]
            if (
                first_sample_time > self.request_end_time
                or last_sample_time < self.request_start_time
            ):
                continue
            return True
        return False

    def get_samples(self):
        if not self.msgs:
            return self.samples

        total = sum([len(m["data"]) for m in self.msgs])
        count = 0
        self.samples = samples = [None] * total
        for msg in self.msgs:
            for samp_time, x, y, z in msg["data"]:
                if samp_time < self.request_start_time:
                    continue
                if samp_time > self.request_end_time:
                    break
                samples[count] = Accel_Measurement(samp_time, x, y, z)
                count += 1
        del samples[count:]
        return self.samples

    def write_to_file(self, filename):
        def do_write():
            try:
                os.nice(20) # Lower process priority
            except AttributeError:
                pass # os.nice not available on all platforms
            except OSError:
                pass # May fail if permissions are insufficient
                
            try:
                with open(filename, "w") as f:
                    f.write("#time,accel_x,accel_y,accel_z\n")
                    samples = self.samples or self.get_samples()
                    for t, accel_x, accel_y, accel_z in samples:
                        f.write(f"{t:.6f},{accel_x:.6f},{accel_y:.6f},{accel_z:.6f}\n")
            except OSError as e:
                logging.warning(f"Failed to write accelerometer data to {filename}: {e}")

        write_proc = multiprocessing.Process(target=do_write)
        write_proc.daemon = True
        write_proc.start()


class APIDumpHelper:
    """
    Handles buffering and sending data to web clients (e.g., for graphs).
    """
    def __init__(self, printer, start, stop, update):
        self.printer = printer
        self.start = start
        self.stop = stop
        self.update = update
        self.interval = 0.05
        self.clients = []
        self.stream = None
        self.timer = None
        self.buffer = []

    def _start_stop(self):
        if not self.stream and self.clients:
            self.stream = self.start()
            reactor = self.printer.get_reactor()
            self.timer = reactor.register_timer(
                self._process, reactor.monotonic() + self.interval
            )
        elif self.stream is not None and not self.clients:
            self.stop(self.stream)
            self.stream = None
            self.printer.get_reactor().unregister_timer(self.timer)
            self.timer = None

    def _process(self, eventtime):
        if self.update is not None:
            self.update(self, eventtime)
        if self.buffer:
            for cb in list(self.clients):
                if not cb(self.buffer):
                    self.clients.remove(cb)
                    self._start_stop()
            self.buffer = []
        return eventtime + self.interval

    def add_client(self, client):
        self.clients.append(client)
        self._start_stop()

    def add_web_client(self, web_request, formatter=lambda v: v):
        client_connection = web_request.get_client_connection()
        template = web_request.get_dict("response_template", {})

        def web_callback(items):
            if client_connection.is_closed():
                return False
            tmp = dict(template)
            tmp["params"] = formatter(items)
            client_connection.send(tmp)
            return True

        self.add_client(web_callback)
        return client_connection


class BeaconTracker:
    """
    Main registration class that manages all Beacon sensors
    and handles G-Code/API dispatch.
    """
    def __init__(self, config, printer):
        self.config = config
        self.printer = printer
        self.sensors = {}
        self.gcodes = {}
        self.endpoints = {}
        self.gcode = printer.lookup_object("gcode")
        self.webhooks = printer.lookup_object("webhooks")

    def get_status(self, eventtime):
        return {"sensors": list(self.sensors.keys())}

    def home_dir(self):
        return os.path.dirname(os.path.realpath(__file__))

    def add_sensor(self, name):
        if name is None:
            cfg = self.config.getsection("beacon")
        else:
            if not name.islower():
                raise self.config.error(
                    f"Beacon sensor name must be all lower case, sensor name '{name}' is not valid"
                )
            cfg = self.config.getsection(f"beacon sensor {name}")
            
        self.sensors[name] = sensor = BeaconProbe(cfg, BeaconId(name, self))
        
        if name is None:
            self.printer.add_object("probe", BeaconProbeWrapper(sensor))
            
        coil_name = "beacon_coil" if name is None else f"beacon_{name}_coil"
        temp_wrapper = BeaconTempWrapper(sensor)
        self.printer.add_object(f"temperature_sensor {coil_name}", temp_wrapper)
        
        try:
            pheaters = self.printer.load_object(self.config, "heaters")
            pheaters.available_sensors.append(f"temperature_sensor {coil_name}")
        except self.printer.config_error as e:
            logging.info(f"Could not add beacon sensor to heaters: {e}")
            
        return sensor

    def get_or_add_sensor(self, name):
        return self.sensors.get(name) or self.add_sensor(name)

    def register_gcode_command(self, sensor, cmd, func, desc):
        if cmd not in self.gcodes:
            handlers = self.gcodes[cmd] = {}
            self.gcode.register_command(
                cmd, lambda gcmd: self.dispatch_gcode(handlers, gcmd), desc=desc
            )
        self.gcodes[cmd][sensor] = func

    def dispatch_gcode(self, handlers, gcmd):
        sensor_name = gcmd.get("SENSOR", "")
        if sensor_name == "":
            sensor_name = None # Use default sensor
            
        handler = handlers.get(sensor_name, None)
        
        if not handler:
            if sensor_name is None:
                raise gcmd.error(
                    "No default Beacon registered, provide SENSOR= option to select specific sensor."
                )
            else:
                raise gcmd.error(
                    f"Requested sensor '{sensor_name}' not found, specify a valid sensor."
                )
        handler(gcmd)

    def register_endpoint(self, sensor, path, callback):
        if path not in self.endpoints:
            self.webhooks.register_endpoint(path, self.dispatch_webhook)
            self.endpoints[path] = {}
        self.endpoints[path][sensor] = callback

    def dispatch_webhook(self, req):
        handlers = self.endpoints[req.method]
        sensor_name = req.get("sensor", "")
        if sensor_name == "":
            sensor_name = None # Use default sensor
            
        handler = handlers.get(sensor_name, None)
        
        if not handler:
            if sensor_name is None:
                raise req.error(
                    "No default Beacon registered, provide 'sensor' option to specify sensor."
                )
            else:
                raise req.error(
                    f"Requested sensor '{sensor_name}' not found, specify a valid or no sensor to use default"
                )
        handler(req)


class BeaconId:
    """
    A helper object passed to BeaconProbe to identify it
    with the BeaconTracker.
    """
    def __init__(self, name, tracker):
        self.name = name
        self.tracker = tracker

    def is_unnamed(self):
        return self.name is None

    def register_command(self, cmd, func, desc):
        self.tracker.register_gcode_command(self.name, cmd, func, desc)

    def register_endpoint(self, path, callback):
        self.tracker.register_endpoint(self.name, path, callback)


# --- Klipper Registration Functions ---

def get_beacons(config):
    printer = config.get_printer()
    beacons = printer.lookup_object("beacons", None)
    if beacons is None:
        beacons = BeaconTracker(config, printer)
        printer.add_object("beacons", beacons)
    return beacons


def load_config(config):
    return get_beacons(config).get_or_add_sensor(None)


def load_config_prefix(config):
    beacons = get_beacons(config)
    sensor = None
    secname = config.get_name()
    parts = secname[7:].split()

    if len(parts) != 0 and parts[0] == "sensor":
        if len(parts) < 2:
            raise config.error("Missing Beacon sensor name")
        sensor = parts[1]
        parts = parts[2:]

    beacon = beacons.get_or_add_sensor(sensor)

    if len(parts) == 0:
        return beacon

    if parts[0] == "model":
        if len(parts) != 2:
            raise config.error(f"Missing Beacon model name in section '{secname}'")
        name = parts[1]
        model = BeaconModel.load(name, config, beacon)
        beacon._register_model(name, model)
        return model
    else:
        raise config.error(f"Unknown beacon config directive '{secname}'")
