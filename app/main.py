import io
import av
import make87 as m87
from make87_messages.video.any_pb2 import FrameAny
from make87_messages.image.jpeg_pb2 import JpegImage
from PIL import Image


class VideoStreamProcessor:
    def __init__(self):
        self.codec_context = None  # Decoder state
        self.received_keyframe = False  # Ensure we start decoding at a keyframe

    def initialize_decoder(self, codec_name: str):
        """Initialize the codec decoder only once per stream."""
        self.codec_context = av.CodecContext.create(codec_name, "r")

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

        # Create a packet and send it to the decoder
        packet = av.Packet(submessage.data)
        try:
            frames = self.codec_context.decode(packet)
            if not packet.is_keyframe:
                return  # Skip non-keyframes
            for frame in frames:
                if isinstance(frame, av.VideoFrame):
                    jpeg_data = self.convert_frame_to_jpeg(frame)
                    jpeg_message = JpegImage(header=submessage.header, data=jpeg_data)
                    publisher.publish(jpeg_message)  # Send JPEG over make87
                    return  # Only process the first valid frame per packet
        except Exception as e:
            print(f"Decoder error: {e}")

    @staticmethod
    def convert_frame_to_jpeg(frame: av.VideoFrame) -> bytes:
        """Convert an AVFrame (PyAV frame) to a JPEG-encoded bytes object."""
        img = frame.to_image()  # Convert to PIL Image
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG", quality=95)  # Save as JPEG in memory
        return buffer.getvalue()  # Return JPEG bytes


# Initialize make87
m87.initialize()

# Setup publisher
publisher = m87.get_publisher(name="JPEG_STREAM", message_type=JpegImage)

# Create the video processor instance with subsampling (e.g., every 5th frame)
processor = VideoStreamProcessor()

# Subscribe to video frames
subscriber = m87.get_subscriber(name="VIDEO_DATA", message_type=FrameAny)
subscriber.subscribe(processor.process_frame)

# Start event loop
m87.loop()
