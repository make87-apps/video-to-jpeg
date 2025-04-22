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
        """Process incoming video frame packets and decode them into JPEGs, applying subsampling."""
        # Identify codec
        video_type = message.WhichOneof("data")
        if video_type == "h264":
            codec_name = "h264"
            submessage = message.h264
        elif video_type == "h265":
            codec_name = "hevc"
            submessage = message.h265
        elif video_type == "av1":
            codec_name = "av1"
            submessage = message.av1
        else:
            print("Unknown frame type received, discarding.")
            return

        # Initialize decoder on first frame
        if self.codec_context is None:
            self.initialize_decoder(codec_name)

        # Wait for a keyframe to start decoding
        if not self.received_keyframe:
            if not submessage.is_keyframe:
                print("Dropping non-keyframe as we haven't received a keyframe yet.")
                return  # Skip until first keyframe arrives
            self.received_keyframe = True
            print("Received first keyframe, starting decoding.")

        try:
            self._header_queue.put(message.header)
            self.ffmpeg_proc.stdin.write(data)
        except Exception as e:
            print(f"Decoder error: {e}")

    @staticmethod
    def convert_frame_to_jpeg(frame: av.VideoFrame) -> bytes:
        """Convert an AVFrame (PyAV frame) to a JPEG-encoded bytes object."""
        img = frame.to_image()  # Convert to PIL Image
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG", quality=95)  # Save as JPEG in memory
        return buffer.getvalue()  # Return JPEG bytes


def main():
    # Initialize make87
    m87.initialize()

    # Setup publisher
    publisher = m87.get_publisher(name="JPEG_IMAGE", message_type=ImageJPEG)

    # Create the video processor instance with subsampling (e.g., every 5th frame)
    processor = VideoStreamProcessor(publisher)

    # Subscribe to video frames
    subscriber = m87.get_subscriber(name="VIDEO_DATA", message_type=FrameAny)
    subscriber.subscribe(processor.process_frame)

    # Start event loop
    m87.loop()


if __name__ == "__main__":
    main()
