import json
import queue
import pyaudio
import requests
import wave
import io
from concurrent.futures import ThreadPoolExecutor
from PyQt6.QtCore import QThread, QObject, pyqtSignal, QMutex, QTimer
from config import TTS_API_URL, TTS_REF_AUDIO_PATH, TTS_PROMPT_TEXT, TTS_PROMPT_LANG

# Update the URL to the open-source TTS API endpoint
url = TTS_API_URL if hasattr(globals(), 'TTS_API_URL') else "http://127.0.0.1:9880/tts"

class TTSThread(QThread):
    """TTS播放线程，负责播放音频数据"""
    
    # 添加实际播放开始的信号
    audio_playback_started = pyqtSignal(bytes, object)  # 音频数据和附加信息
    
    def __init__(self, audio_config):
        super().__init__()
        self.audio_config = audio_config
        self.audio_queue = queue.Queue()
        self.is_running = True
        self.mutex = QMutex()
        self.full_audio = b""
        
    def run(self):
        """线程主函数，负责播放队列中的音频数据"""
        p = pyaudio.PyAudio()
        stream = p.open(
            format=self.audio_config['format'],
            channels=self.audio_config['channels'],
            rate=self.audio_config['rate'],
            output=True
        )

        try:
            while self.is_running:
                try:
                    # 从队列获取音频数据和元数据
                    data = self.audio_queue.get(timeout=0.1)
                    if not data:
                        continue
                        
                    # 解包数据和元数据
                    if isinstance(data, tuple) and len(data) == 2:
                        audio_data, metadata = data
                    else:
                        audio_data, metadata = data, None
                    
                    # 发出实际播放开始信号
                    if audio_data:
                        self.audio_playback_started.emit(audio_data, metadata)
                    
                    # 播放音频数据
                    stream.write(audio_data)
                    
                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"[播放错误] {str(e)}")

        finally:
            stream.stop_stream()
            stream.close()
            p.terminate()
    
    def stop(self):
        """停止线程"""
        self.is_running = False
        self.wait()
    
    # 修改add_audio方法，支持元数据
    def add_audio(self, audio_data, metadata=None):
        """添加音频数据到队列"""
        self.audio_queue.put((audio_data, metadata))
        self.full_audio += audio_data
    
    def clear_queue(self):
        """清空音频队列"""
        try:
            while not self.audio_queue.empty():
                self.audio_queue.get_nowait()
        except queue.Empty:
            pass
            
    def is_queue_empty(self):
        """检查队列是否为空"""
        return self.audio_queue.empty()
    
    def cancel_request_id(self, request_id):
        """取消特定请求ID的所有待播放音频"""
        if not request_id:
            return
            
        # 创建新队列并过滤数据
        new_queue = queue.Queue()
        cancelled_count = 0
        
        # 加锁防止并发问题
        self.mutex.lock()
        try:
            # 逐个检查队列中的项目
            while not self.audio_queue.empty():
                try:
                    item = self.audio_queue.get_nowait()
                    if not item:
                        continue
                        
                    # 检查元数据中的请求ID
                    if isinstance(item, tuple) and len(item) == 2:
                        audio_data, metadata = item
                        # 如果元数据是元组且包含请求ID
                        if isinstance(metadata, tuple) and len(metadata) >= 2 and metadata[1] == request_id:
                            cancelled_count += 1
                            continue
                    
                    # 保留不匹配的项目
                    new_queue.put(item)
                    
                except queue.Empty:
                    break
            
            # 替换原队列
            self.audio_queue = new_queue
            
        finally:
            self.mutex.unlock()
        
        if cancelled_count > 0:
            print(f"已从播放队列中移除 {cancelled_count} 条过时音频")

class TTSManager(QObject):
    # 修改信号：添加request_id参数
    tts_playback_started = pyqtSignal(str, str)  # (text, request_id)
    # 添加新信号：实际播放开始信号
    tts_audio_playback_started = pyqtSignal(str, str)  # (text, request_id)
    
    def __init__(self):
        super().__init__()
        # 音频配置 - 与API请求中的配置保持一致
        self.audio_config = {
            'channels': 1,
            'rate': 32000,  # 32kHz sample rate
            'format': pyaudio.paInt16  # 16位PCM
        }
        
        # 创建播放线程
        self.player_thread = TTSThread(self.audio_config)
        self.player_thread.start()
        
        # 连接实际播放开始信号
        self.player_thread.audio_playback_started.connect(self._on_audio_playback_started)
        
        # 创建线程池用于并发TTS请求
        self.thread_pool = ThreadPoolExecutor(max_workers=3)
        
        # 当前请求是否正在进行
        self.is_requesting = False
        self.is_processing = False  # 添加此属性来跟踪处理状态
        
        # 修改请求队列结构，包含文本和请求ID
        self.request_queue = []  # [(text, request_id, emotion), ...]
        self.active_requests = set()  # 跟踪活动的请求ID
        
        # 设置默认参考音频和提示文本
        self.ref_audio_path = TTS_REF_AUDIO_PATH if hasattr(globals(), 'TTS_REF_AUDIO_PATH') else "/home/hsiaofe/Desktop/Voice Sample/雷电将军.wav"
        self.prompt_text = TTS_PROMPT_TEXT if hasattr(globals(), 'TTS_PROMPT_TEXT') else "哎呀，你不会怕了吧。明明此世最为殊胜最为恐怖的雷霆化身就站在你身边。"
        self.prompt_lang = TTS_PROMPT_LANG if hasattr(globals(), 'TTS_PROMPT_LANG') else "zh"

    def is_queue_empty(self) -> bool:
        """
        检查是否还有音频在队列中等待播放
        
        Returns:
            bool: True 表示队列为空（没有音频在播放或等待），False 表示队列非空
        """
        return self.player_thread.is_queue_empty() and len(self.request_queue) == 0

    def build_tts_params(self, text: str, emotion: str = "neutral") -> dict:
        """
        构建请求参数
        
        Args:
            text: 要转换的文本
            emotion: 情绪类型，用于选择合适的参数配置
            
        Returns:
            dict: 请求参数字典
        """
        # 根据情绪可以调整一些参数如temperature, repetition_penalty等
        # 这里简单映射，实际使用时可以进一步调整
        params = {
            "text": text,
            "text_lang": "zh",  # 默认使用中文，可以从配置中读取
            "ref_audio_path": self.ref_audio_path,
            "prompt_text": self.prompt_text,
            "prompt_lang": self.prompt_lang,
            "text_split_method": "cut5",
            "batch_size": 1,
            "streaming_mode": True
        }
        
        # 根据情绪调整参数
        if emotion == "happy":
            params["speed_factor"] = 1.1  # 快一点
            params["temperature"] = 1.1  # 增加随机性
        elif emotion == "sad":
            params["speed_factor"] = 0.9  # 慢一点
            params["temperature"] = 0.9  # 降低随机性
        elif emotion == "angry":
            params["speed_factor"] = 1.2  # 更快
            params["temperature"] = 1.2  # 更高随机性
        
        return params

    def request_tts(self, text: str, request_id: str = None, emotion: str = "neutral"):
        """
        发起TTS请求
        
        Args:
            text: 要转换的文本
            request_id: 请求标识符，用于跟踪特定请求的TTS
            emotion: 情绪类型，用于调整语音风格
        """
        if not text or not text.strip():
            return
        
        # 如果没有提供请求ID，生成一个特殊标记
        if request_id is None:
            request_id = "default_request"
        
        # 将请求添加到队列，包含文本、请求ID和情绪
        self.request_queue.append((text, request_id, emotion))
        print(f"已添加TTS请求到队列: '{text[:20]}...' (请求ID: {request_id}, 情绪: {emotion})")
        
        # 如果当前没有处理中的请求，开始处理
        if not self.is_processing:
            self._process_next_request()
    
    def _on_audio_playback_started(self, audio_data, metadata):
        """处理音频实际开始播放事件"""
        if metadata and isinstance(metadata, tuple) and len(metadata) == 2:
            text, request_id = metadata
            # 发送实际播放开始信号
            self.tts_audio_playback_started.emit(text, request_id)
    
    def _process_next_request(self):
        """处理队列中的下一个TTS请求"""
        if not self.request_queue:
            self.is_processing = False
            return
            
        # 获取下一个请求
        if len(self.request_queue[0]) == 3:
            text, request_id, emotion = self.request_queue.pop(0)
        else:
            # 兼容旧格式
            text, request_id = self.request_queue.pop(0)
            emotion = "neutral"
        
        print(f"开始处理TTS请求: '{text[:20]}...' (请求ID: {request_id}, 情绪: {emotion})")
        
        # 将请求ID添加到活动请求集合
        self.active_requests.add(request_id)
        
        # 构建参数
        params = self.build_tts_params(text, emotion)
        
        # 提交到线程池执行
        self.thread_pool.submit(self._execute_tts_request, text, request_id, emotion, params)
        
        # 立即处理下一个请求，不等待当前请求完成
        if self.request_queue:
            QTimer.singleShot(100, self._process_next_request)

    def _execute_tts_request(self, text, request_id, emotion, params):
        """在线程池中执行TTS请求"""
        try:
            # 发送GET请求到TTS API
            response = requests.get(url, params=params, stream=True)
            
            if response.status_code != 200:
                print(f"[TTS请求错误] 状态码: {response.status_code}, 消息: {response.text}")
                raise Exception(f"API返回错误: {response.status_code}")
            
            # 处理音频流响应
            audio_data = response.content
            
            # 如果请求已被取消，则不添加到播放队列
            if request_id not in self.active_requests:
                print(f"请求 {request_id} 已被取消，不添加到播放队列")
                return
                
            # 将PCM数据添加到播放队列
            self.player_thread.add_audio(audio_data, (text, request_id))
            
            # 发送队列添加信号
            self.tts_playback_started.emit(text, request_id)
                                
        except Exception as e:
            print(f"[TTS请求错误] {str(e)}")
        finally:
            # 请求完成后从活动集合中移除
            if request_id in self.active_requests:
                self.active_requests.remove(request_id)

    def stop_playing(self):
        """停止当前播放并清空队列"""
        self.request_queue = []
        self.is_processing = False
        self.active_requests.clear()  # 清空活动请求集合
        self.player_thread.clear_queue()
        print("已停止所有TTS播放和请求")

    def cancel_request_id(self, request_id: str):
        """
        取消特定请求ID的所有TTS请求
        """
        print(f"取消请求ID为 {request_id} 的所有TTS请求")
        
        # 过滤掉队列中指定请求ID的项目
        self.request_queue = [(text, rid, emotion) for text, rid, emotion in self.request_queue if rid != request_id]
        
        # 从活动请求集合中移除
        if request_id in self.active_requests:
            self.active_requests.remove(request_id)
        
        # 清理播放队列中的过时音频
        self.player_thread.cancel_request_id(request_id)
        
        # 如果还有其他请求，确保处理继续
        if self.request_queue and not any(rid in self.active_requests for _, rid, _ in self.request_queue):
            QTimer.singleShot(100, self._process_next_request)

    def stop(self):
        """停止播放并清理资源"""
        self.player_thread.stop()
        
    def get_audio(self) -> bytes:
        """获取收集的完整音频数据"""
        return self.player_thread.full_audio

# 测试函数，用于验证TTS功能
def test_tts_api():
    """测试TTS API功能"""
    import time
    
    print("\n====== 开始测试TTS功能 ======")
    print(f"TTS API URL: {url}")
    print(f"参考音频路径: {TTS_REF_AUDIO_PATH}")
    print(f"提示文本: {TTS_PROMPT_TEXT}")
    
    # 创建TTS管理器实例
    tts_manager = TTSManager()
    
    # 定义测试文本
    test_texts = [
        ("这是一个测试句子，用于验证TTS功能是否正常工作。", "正常"),
        ("我非常高兴能够为您服务！", "高兴"),
        ("我感到有些失落和悲伤...", "悲伤"),
        ("这太令人气愤了！我无法接受这种情况！", "愤怒")
    ]
    
    # 连接信号处理函数
    def on_tts_playback_started(text, request_id):
        print(f"\n[事件] TTS开始播放: '{text[:30]}...' (ID: {request_id})")
    
    def on_audio_playback_started(text, request_id):
        print(f"[事件] 音频实际开始播放: '{text[:30]}...' (ID: {request_id})")
    
    tts_manager.tts_playback_started.connect(on_tts_playback_started)
    tts_manager.tts_audio_playback_started.connect(on_audio_playback_started)
    
    # 测试不同情绪的TTS
    for i, (text, emotion) in enumerate(test_texts):
        request_id = f"test_{i+1}"
        print(f"\n[测试 {i+1}] 请求TTS: '{text}' (情绪: {emotion})")
        
        # 映射情绪到参数
        emotion_param = "neutral"
        if (emotion == "高兴"):
            emotion_param = "happy"
        elif (emotion == "悲伤"):
            emotion_param = "sad"
        elif (emotion == "愤怒"):
            emotion_param = "angry"
            
        # 请求TTS
        tts_manager.request_tts(text, request_id, emotion_param)
        
        # 等待5秒让音频播放
        print(f"[等待] 播放音频中...")
        time.sleep(5)
    
    # 等待所有音频播放完毕
    print("\n[等待] 等待所有音频播放完毕...")
    while not tts_manager.is_queue_empty():
        time.sleep(0.5)
    
    # 测试取消功能
    cancel_text = "这是一个将被取消的语音请求，它不应该被播放出来。"
    cancel_id = "cancel_test"
    print(f"\n[测试取消] 请求TTS并立即取消: '{cancel_text}'")
    tts_manager.request_tts(cancel_text, cancel_id)
    time.sleep(0.1)  # 稍微等待一下，确保请求被添加到队列
    tts_manager.cancel_request_id(cancel_id)
    print("[测试取消] 已取消请求")
    
    # 测试结束，停止TTS线程
    time.sleep(1)
    tts_manager.stop()
    print("\n====== TTS功能测试完成 ======")

# 如果直接运行此文件，则执行测试
if __name__ == "__main__":
    test_tts_api()