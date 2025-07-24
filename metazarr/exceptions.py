class MetaZarrError(Exception):
    """基类"""

class ValidationError(MetaZarrError):
    """用户输入校验相关"""

class ConversionError(MetaZarrError):
    """原始文件转换失败"""

class RangeError(MetaZarrError):
    """检索裁剪出界"""
