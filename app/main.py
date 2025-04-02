import os
import subprocess
import threading
from make87_messages.image.compressed.image_jpeg_pb2 import ImageJPEG
from make87_messages.video.any_pb2 import FrameAny
from make87_messages.core.header_pb2 import Header
import make87
from queue import SimpleQueue, Empty as EmptyException


def get_qsv_decode_support():
    """
    Check ffmpeg decoders for QSV support for h264, hevc, and av1.
    Returns a dict keyed by codec with info on whether QSV decoding is available.
    """
    codec_info = {
        "h264": "h264_qsv",
        "hevc": "hevc_qsv",
        "av1": "av1_qsv",
    }
    try:
        result = subprocess.run(["ffmpeg", "-decoders"], capture_output=True, text=True, check=True)
        decoder_lines = result.stdout.splitlines()
    except subprocess.CalledProcessError:
        decoder_lines = []

    ffmpeg_decoders = set()
    for line in decoder_lines:
        line = line.strip()
        # Lines starting with "V" are video decoders; get the decoder name.
        if line.startswith("V") and len(line.split()) > 1:
            ffmpeg_decoders.add(line.split()[1])

    final_info = {}
    for codec, qsv_decoder in codec_info.items():
        final_info[codec] = {
            "qsv_supported": qsv_decoder in ffmpeg_decoders,
            "ffmpeg_decoder": qsv_decoder,
        }
    return final_info


def get_qsv_encode_support():
    """
    Check ffmpeg encoders for QSV support for JPEG (mjpeg_qsv).
    Returns a dict with a boolean indicating if QSV encoding is available.
    """
    encoder_name = "mjpeg_qsv"
    try:
        result = subprocess.run(["ffmpeg", "-encoders"], capture_output=True, text=True, check=True)
        encoder_lines = result.stdout.splitlines()
    except subprocess.CalledProcessError:
        encoder_lines = []

    ffmpeg_encoders = set()
    for line in encoder_lines:
        line = line.strip()
        # Lines starting with "V" are video encoders.
        if line.startswith("V") and len(line.split()) > 1:
            ffmpeg_encoders.add(line.split()[1])

    return {"mjpeg_qsv": encoder_name in ffmpeg_encoders}


class VideoStreamProcessor:
    def __init__(self, publisher):
        self.publisher = publisher
        self._header_queue = SimpleQueue()
        # Get QSV decode and encode capabilities.
        self.qsv_decode_capabilities = get_qsv_decode_support()
        self.qsv_encode_capabilities = get_qsv_encode_support()
        self.codec_type = None  # Set on first valid frame.
        self.ffmpeg_proc = None
        self.stdout_thread = None
        self.stderr_thread = None
        self.received_keyframe = False

        print("QSV decode capabilities detected:")
        for codec, caps in self.qsv_decode_capabilities.items():
            print(
                f"  {codec.upper()} - QSV decoding supported: {caps['qsv_supported']}, FFmpeg decoder: {caps['ffmpeg_decoder']}"
            )
        print("QSV encoding (JPEG) supported:", self.qsv_encode_capabilities.get("mjpeg_qsv"))

    def start_ffmpeg(self, codec):
        ffmpeg_cmd = ["ffmpeg"]

        # Determine if we can do hardware decode for this codec.
        decode_hw = self.qsv_decode_capabilities.get(codec, {}).get("qsv_supported", False)
        # Determine if we can do hardware encoding (for mjpeg).
        encode_hw = self.qsv_encode_capabilities.get("mjpeg_qsv", False)

        # If either side uses QSV encoding or decoding, we need to set up the QSV device.
        if decode_hw:
            # For QSV decoding, enable hardware acceleration.
            ffmpeg_cmd += [
                "-load_plugin",
                "hevc_hw",
                "-hwaccel",
                "qsv",
                "-qsv_device",
                make87.resolve_peripheral_name("RENDER"),
                "-hwaccel_output_format",
                "qsv",
            ]
        elif encode_hw:
            # If decoding is done in software but we want to encode via QSV, we still need to set the device.
            ffmpeg_cmd += [
                "-load_plugin",
                "hevc_hw",
                "-qsv_device",
                make87.resolve_peripheral_name("RENDER"),
            ]
        else:
            # All software – no extra QSV options.
            pass

        # Input from stdin.
        ffmpeg_cmd += ["-i", "-"]

        # Determine if a conversion filter is required.
        # Case 1: SW decode -> QSV encode: upload frames to QSV (hwupload).
        # Case 2: QSV decode -> SW encode: download frames from QSV (hwdownload).
        if (not decode_hw) and encode_hw:
            ffmpeg_cmd += ["-vf", "format=nv12,hwupload=qsv"]
        elif decode_hw and (not encode_hw):
            ffmpeg_cmd += ["-vf", "hwdownload,format=nv12"]
        # If both are hardware or both are software, no conversion filter is needed.

        # Set output encoder.
        if encode_hw:
            encoder = "mjpeg_qsv"
        else:
            encoder = "mjpeg"

        ffmpeg_cmd += ["-c:v", encoder, "-q:v", "2", "-f", "image2pipe", "-"]

        print("Running FFmpeg command:", " ".join(ffmpeg_cmd))
        self.ffmpeg_proc = subprocess.Popen(
            ffmpeg_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        self.stdout_thread = threading.Thread(target=self.read_ffmpeg_stdout, daemon=True)
        self.stdout_thread.start()
        self.stderr_thread = threading.Thread(target=self.read_ffmpeg_stderr, daemon=True)
        self.stderr_thread.start()

    def read_ffmpeg_stdout(self):
        """
        Continuously reads ffmpeg's stdout, extracts JPEG images, and publishes them.
        JPEG images are delimited by the markers 0xFFD8 (start) and 0xFFD9 (end).
        """
        buffer = bytearray()
        chunk_size = 2_000_000

        while True:
            try:
                chunk = self.ffmpeg_proc.stdout.read(chunk_size)
                if not chunk:
                    break
                buffer.extend(chunk)

                # Remove any leading data before the JPEG start marker.
                start = buffer.find(b"\xff\xd8")
                if start > 0:
                    del buffer[:start]

                # Look for complete JPEG images in the buffer.
                while True:
                    start = buffer.find(b"\xff\xd8")
                    if start == -1:
                        break
                    end = buffer.find(b"\xff\xd9", start + 2)
                    if end == -1:
                        break

                    jpeg_data = bytes(buffer[start : end + 2])
                    try:
                        header = self._header_queue.get_nowait()
                    except EmptyException:
                        header = Header()
                        header.timestamp.GetCurrentTime()
                    jpeg_message = ImageJPEG(header=header, data=jpeg_data)
                    self.publisher.publish(jpeg_message)
                    del buffer[: end + 2]
            except Exception as e:
                print(f"Error reading ffmpeg stdout: {e}")
                break

    def read_ffmpeg_stderr(self):
        """Continuously reads ffmpeg's stderr and prints each line to the terminal."""
        while True:
            line = self.ffmpeg_proc.stderr.readline()
            if not line:
                break
            print(line.decode("utf-8"), end="")

    def process_frame(self, message: FrameAny):
        """
        Processes incoming video frame packets by writing the packet data
        into ffmpeg's stdin.
        Waits for the first keyframe to start feeding packets.
        """
        video_type = message.WhichOneof("data")
        codec_map = {"h264": "h264", "h265": "hevc", "av1": "av1"}

        if video_type not in codec_map:
            print("Unknown frame type received, discarding.")
            return

        codec = codec_map[video_type]
        data = getattr(message, video_type).data
        is_keyframe = getattr(message, video_type).is_keyframe

        # Wait for the first keyframe to ensure correct decoding.
        if not self.received_keyframe:
            if self.ffmpeg_proc is None:
                self.codec_type = codec
                self.start_ffmpeg(codec)

            if not is_keyframe:
                print("Dropping non-keyframe as we haven't received a keyframe yet.")
                return
            self.received_keyframe = True
            print("Received first keyframe, starting ffmpeg feeding.")

        try:
            self._header_queue.put(message.header)
            self.ffmpeg_proc.stdin.write(data)
        except Exception as e:
            print(f"Error writing to ffmpeg stdin: {e}")


def main():
    make87.initialize()
    publisher = make87.get_publisher(name="JPEG_IMAGE", message_type=ImageJPEG)
    processor = VideoStreamProcessor(publisher)
    subscriber = make87.get_subscriber(name="VIDEO_DATA", message_type=FrameAny)
    subscriber.subscribe(processor.process_frame)
    make87.loop()
