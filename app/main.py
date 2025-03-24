import os
import subprocess
import threading
from make87_messages.image.compressed.image_jpeg_pb2 import ImageJPEG
from make87_messages.video.any_pb2 import FrameAny
from make87_messages.core.header_pb2 import Header
import make87
from queue import SimpleQueue, Empty as EmptyException


def detect_vaapi_driver():
    try:
        result = subprocess.run(
            ["vainfo"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # <---- merge stderr into stdout
            text=True,
            check=True,
        )
        lines = result.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        print("vainfo failed, defaulting to iHD")
        return "iHD"

    active_driver = "iHD"
    candidate = None

    for line in lines:
        line = line.strip()
        if "Trying to open" in line and "_drv_video.so" in line:
            candidate = line.split("/")[-1].split("_drv_video.so")[0]
        elif "va_openDriver() returns 0" in line and candidate:
            active_driver = candidate
            break

    print(f"Detected active VAAPI driver: {active_driver}")
    return active_driver


def get_vaapi_decode_support():
    codec_info = {
        "h264": ("VAProfileH264", "h264_vaapi"),
        "hevc": ("VAProfileHEVC", "hevc_vaapi"),
        "av1": ("VAProfileAV1", "av1_vaapi"),
    }
    try:
        result = subprocess.run(["vainfo"], capture_output=True, text=True, check=True)
        vainfo_lines = result.stdout.splitlines()
    except subprocess.CalledProcessError:
        vainfo_lines = []

    vaapi_support = {}
    for codec, (va_profile, _) in codec_info.items():
        vaapi_support[codec] = any(va_profile in line and "VAEntrypointVLD" in line for line in vainfo_lines)

    try:
        result = subprocess.run(["ffmpeg", "-decoders"], capture_output=True, text=True, check=True)
        decoder_lines = result.stdout.splitlines()
    except subprocess.CalledProcessError:
        decoder_lines = []

    ffmpeg_decoders = set()
    for line in decoder_lines:
        line = line.strip()
        if line.startswith("V") and len(line.split()) > 1:
            ffmpeg_decoders.add(line.split()[1])

    final_info = {}
    for codec, (_, ffmpeg_name) in codec_info.items():
        final_info[codec] = {
            "vaapi_supported": vaapi_support.get(codec, False),
            "ffmpeg_decoder": ffmpeg_name,
            "ffmpeg_available": ffmpeg_name in ffmpeg_decoders,
        }

    return final_info


class VideoStreamProcessor:
    def __init__(self, publisher):
        self.publisher = publisher
        self._header_queue = SimpleQueue()
        self.vaapi_driver = detect_vaapi_driver()
        self.vaapi_capabilities = get_vaapi_decode_support()
        self.codec_type = None  # Set on first valid frame
        self.ffmpeg_proc = None
        self.stdout_thread = None
        self.stderr_thread = None
        self.received_keyframe = False

        print("VAAPI capabilities detected:")
        for codec, caps in self.vaapi_capabilities.items():
            print(
                f"  {codec.upper()} - VAAPI supported: {caps['vaapi_supported']}, FFmpeg decoder: {caps['ffmpeg_decoder']}, Available: {caps['ffmpeg_available']}"
            )

    def start_ffmpeg(self, codec):
        ffmpeg_cmd = ["ffmpeg"]
        vaapi_info = self.vaapi_capabilities.get(codec, {})

        if vaapi_info.get("vaapi_supported") and vaapi_info.get("ffmpeg_available"):
            print(f"Using VAAPI for {codec} decoding.")
            ffmpeg_cmd += [
                "-hwaccel",
                "vaapi",
                "-hwaccel_output_format",
                "vaapi",
            ]
        else:
            print(f"Falling back to software decoding for {codec}.")

        ffmpeg_cmd += ["-i", "-", "-f", "image2pipe", "-vcodec", "mjpeg", "-q:v", "2", "-"]

        env = os.environ.copy()
        env["LIBVA_DRIVER_NAME"] = self.vaapi_driver

        self.ffmpeg_proc = subprocess.Popen(
            ffmpeg_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
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

                # If the buffer has leading data before the JPEG start marker,
                # remove it to keep the buffer small.
                start = buffer.find(b"\xff\xd8")
                if start > 0:
                    del buffer[:start]

                # Look for complete JPEGs in the current buffer.
                while True:
                    start = buffer.find(b"\xff\xd8")
                    if start == -1:
                        break
                    # Search for the end marker *after* the start marker
                    end = buffer.find(b"\xff\xd9", start + 2)
                    if end == -1:
                        # Incomplete JPEG; wait for more data.
                        break

                    # Yield the complete JPEG image.
                    jpeg_data = bytes(buffer[start : end + 2])
                    try:
                        header = self._header_queue.get_nowait()
                    except EmptyException:
                        header = Header()
                        header.timestamp.GetCurrentTime()
                    jpeg_message = ImageJPEG(header=header, data=jpeg_data)
                    self.publisher.publish(jpeg_message)
                    # Remove the processed image from the buffer.
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
            # Decode and print the stderr log.
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

        # Write the raw packet data into ffmpeg's stdin.
        try:
            self._header_queue.put(message.header)
            self.ffmpeg_proc.stdin.write(data)
            # self.ffmpeg_proc.stdin.flush()
        except Exception as e:
            print(f"Error writing to ffmpeg stdin: {e}")


def main():
    make87.initialize()
    publisher = make87.get_publisher(name="JPEG_IMAGE", message_type=ImageJPEG)
    processor = VideoStreamProcessor(publisher)
    subscriber = make87.get_subscriber(name="VIDEO_DATA", message_type=FrameAny)
    subscriber.subscribe(processor.process_frame)
    make87.loop()


if __name__ == "__main__":
    main()
