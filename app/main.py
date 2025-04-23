import subprocess
import threading
from queue import SimpleQueue, Empty as EmptyException

import make87
from make87_messages.core.header_pb2 import Header
from make87_messages.image.compressed.image_jpeg_pb2 import ImageJPEG
from make87_messages.video.any_pb2 import FrameAny


class VideoStreamProcessor:
    def __init__(self, publisher):
        self.publisher = publisher
        self._header_queue = SimpleQueue()
        self.codec_type = None  # Set on first valid frame.
        self.ffmpeg_proc = None
        self.stdout_thread = None
        self.stderr_thread = None
        self.received_keyframe = False

    def start_ffmpeg(self, codec):
        ffmpeg_cmd = ["ffmpeg"]

        # Input from stdin.
        ffmpeg_cmd += ["-i", "-", "-c:v", "mjpeg", "-q:v", "2", "-f", "image2pipe", "-"]

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
    # Initialize make87
    make87.initialize()

    # Setup publisher
    publisher = make87.get_publisher(name="JPEG_IMAGE", message_type=ImageJPEG)

    # Create the video processor instance with subsampling (e.g., every 5th frame)
    processor = VideoStreamProcessor(publisher)

    # Subscribe to video frames
    subscriber = make87.get_subscriber(name="VIDEO_DATA", message_type=FrameAny)
    subscriber.subscribe(processor.process_frame)

    # Start event loop
    make87.loop()


if __name__ == "__main__":
    main()
