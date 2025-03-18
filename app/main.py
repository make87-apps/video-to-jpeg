import os
import subprocess
import threading
from make87_messages.image.compressed.image_jpeg_pb2 import ImageJPEG
from make87_messages.video.any_pb2 import FrameAny
import make87


class VideoStreamProcessor:
    def __init__(self, publisher):
        self.publisher = publisher
        self.ffmpeg_proc = self.start_ffmpeg()
        # Start a thread to continuously read and extract JPEG frames from ffmpeg's stdout.
        self.stdout_thread = threading.Thread(target=self.read_ffmpeg_stdout, daemon=True)
        self.stdout_thread.start()
        self.received_keyframe = False  # Only start feeding packets after a keyframe is seen

    def start_ffmpeg(self):
        """Starts ffmpeg with HW acceleration, reading from stdin and outputting JPEG images to stdout."""
        ffmpeg_cmd = [
            "ffmpeg",
            "-hwaccel",
            "vaapi",  # Use hardware acceleration (adjust as needed)
            "-hwaccel_output_format",
            "vaapi",
            "-i",
            "-",  # Read input from stdin
            "-f",
            "image2pipe",  # Output as a stream of images
            "-vcodec",
            "mjpeg",  # Encode as JPEG
            "-q:v",
            "2",  # JPEG quality (lower value = better quality)
            "-",  # Write output to stdout
        ]
        # Add the environment variable LIBVA_DRIVER_NAME=i965
        env = os.environ.copy()
        env["LIBVA_DRIVER_NAME"] = "i965"
        return subprocess.Popen(
            ffmpeg_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
        )

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
                    jpeg_message = ImageJPEG(header=None, data=jpeg_data)
                    self.publisher.publish(jpeg_message)
                    # Remove the processed image from the buffer.
                    del buffer[: end + 2]
            except Exception as e:
                print(f"Error reading ffmpeg stdout: {e}")
                break

    def process_frame(self, message: FrameAny):
        """
        Processes incoming video frame packets by writing the packet data
        into ffmpeg's stdin.
        Waits for the first keyframe to start feeding packets.
        """
        video_type = message.WhichOneof("data")
        if video_type == "h264":
            data = message.h264.data
            is_keyframe = message.h264.is_keyframe
        elif video_type == "h265":
            data = message.h265.data
            is_keyframe = message.h265.is_keyframe
        elif video_type == "av1":
            data = message.av1.data
            is_keyframe = message.av1.is_keyframe
        else:
            print("Unknown frame type received, discarding.")
            return

        # Wait for the first keyframe to ensure correct decoding.
        if not self.received_keyframe:
            if not is_keyframe:
                print("Dropping non-keyframe as we haven't received a keyframe yet.")
                return
            self.received_keyframe = True
            print("Received first keyframe, starting ffmpeg feeding.")

        # Write the raw packet data into ffmpeg's stdin.
        try:
            self.ffmpeg_proc.stdin.write(data)
            self.ffmpeg_proc.stdin.flush()
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
