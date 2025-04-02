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
    Returns a dict with info on whether QSV is available.
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
        # Lines starting with "V" are video decoders; get the decoder name
        if line.startswith("V") and len(line.split()) > 1:
            ffmpeg_decoders.add(line.split()[1])

    final_info = {}
    for codec, qsv_decoder in codec_info.items():
        final_info[codec] = {
            "qsv_supported": qsv_decoder in ffmpeg_decoders,
            "ffmpeg_decoder": qsv_decoder,
        }
    return final_info


class VideoStreamProcessor:
    def __init__(self, publisher):
        self.publisher = publisher
        self._header_queue = SimpleQueue()
        # Use QSV capabilities instead of VAAPI.
        self.qsv_capabilities = get_qsv_decode_support()
        self.codec_type = None  # Set on first valid frame
        self.ffmpeg_proc = None
        self.stdout_thread = None
        self.stderr_thread = None
        self.received_keyframe = False

        print("QSV capabilities detected:")
        for codec, caps in self.qsv_capabilities.items():
            print(
                f"  {codec.upper()} - QSV supported: {caps['qsv_supported']}, FFmpeg decoder: {caps['ffmpeg_decoder']}"
            )

    def start_ffmpeg(self, codec):
        ffmpeg_cmd = ["ffmpeg"]
        qsv_info = self.qsv_capabilities.get(codec, {})

        if qsv_info.get("qsv_supported"):
            print(f"Using QSV for {codec} decoding/encoding.")
            ffmpeg_cmd += [
                "-load_plugin",
                "hevc_hw",  # Plugin load required for QSV acceleration.
                "-hwaccel",
                "qsv",
                "-qsv_device",
                make87.resolve_peripheral_name("RENDER"),
                "-hwaccel_output_format",
                "qsv",
            ]
        else:
            print(f"Falling back to software decoding/encoding for {codec}.")

        ffmpeg_cmd += [
            "-i",
            "-",
            # Use the QSV mjpeg encoder if QSV is supported; otherwise, use the standard mjpeg encoder.
            "-c:v",
            "mjpeg_qsv" if qsv_info.get("qsv_supported") else "mjpeg",
            "-q:v",
            "2",
            "-f",
            "image2pipe",
            "-",
        ]

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

                # Remove any leading garbage before the JPEG start marker.
                start = buffer.find(b"\xff\xd8")
                if start > 0:
                    del buffer[:start]

                # Search for complete JPEGs in the buffer.
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
                    # Remove the processed JPEG from the buffer.
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
