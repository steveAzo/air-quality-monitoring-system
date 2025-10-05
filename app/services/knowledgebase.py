from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class AirQualityAssessor:
    def __init__(self):
        # Enhanced standards with more parameters
        self.aqi_standards = {
            "pm25": {
                "good": (0, 12),
                "moderate": (12.1, 35.4),
                "unhealthy_sensitive": (35.5, 55.4),
                "unhealthy": (55.5, 150.4),
                "very_unhealthy": (150.5, 250.4),
                "hazardous": (250.5, 500.4)
            },
            "pm10": {
                "good": (0, 54),
                "moderate": (55, 154),
                "unhealthy_sensitive": (155, 254),
                "unhealthy": (255, 354),
                "very_unhealthy": (355, 424),
                "hazardous": (425, 604)
            },
            "pm1": {
                "good": (0, 12),
                "moderate": (12.1, 35.4),
                "unhealthy_sensitive": (35.5, 55.4),
                "unhealthy": (55.5, 150.4),
                "very_unhealthy": (150.5, 250.4),
                "hazardous": (250.5, 500.4)
            },
            "temperature": {
                "comfortable": (18, 26),
                "moderate": (26.1, 30),
                "uncomfortable": (30.1, 35),
                "dangerous": (35.1, 50)
            },
            "relativehumidity": {
                "comfortable": (30, 60),
                "moderate": (60.1, 70),
                "uncomfortable": (70.1, 85),
                "dangerous": (85.1, 100)
            },
            "um003": {  # Ultra-fine particles
                "good": (0, 1000),
                "moderate": (1001, 5000),
                "unhealthy_sensitive": (5001, 10000),
                "unhealthy": (10001, 20000),
                "very_unhealthy": (20001, 50000),
                "hazardous": (50001, 100000)
            }
        }
        
        # Parameter priority for overall assessment (most critical first)
        self.parameter_priority = ["pm25", "pm10", "pm1", "um003", "temperature", "relativehumidity"]
        
        self.health_messages = {
            "good": "Air quality is satisfactory with little to no risk.",
            "moderate": "Acceptable air quality, but sensitive groups may experience minor effects.",
            "unhealthy_sensitive": "Members of sensitive groups may experience health effects.",
            "unhealthy": "Everyone may begin to experience health effects.",
            "very_unhealthy": "Health alert: everyone may experience more serious health effects.",
            "hazardous": "Health warning of emergency conditions.",
            "comfortable": "Conditions are comfortable and ideal.",
            "uncomfortable": "Conditions may cause discomfort for some people.",
            "dangerous": "Conditions pose health risks, take precautions."
        }
    
    def get_air_quality_level(self, parameter: str, value: float) -> Dict:
        """Get air quality level for a specific parameter"""
        # Normalize parameter names
        normalized_param = parameter.lower().replace(" ", "").replace("_", "")
        
        # Map variations to standard names
        param_mapping = {
            "rh": "relativehumidity",
            "humidity": "relativehumidity",
            "temp": "temperature",
            "ultrafine": "um003",
            "particles": "um003"
        }
        
        actual_param = param_mapping.get(normalized_param, normalized_param)
        
        if actual_param not in self.aqi_standards:
            return {
                "level": "unknown",
                "message": "No assessment available for this parameter",
                "color": "#666666"
            }
        
        standards = self.aqi_standards[actual_param]
        
        for level, (min_val, max_val) in standards.items():
            if min_val <= value <= max_val:
                return {
                    "level": level,
                    "message": self.health_messages.get(level, "No specific health message available"),
                    "color": self._get_color_code(level)
                }
        
        return {
            "level": "unknown",
            "message": "Value outside assessment range",
            "color": "#666666"
        }
    
    def _get_color_code(self, level: str) -> str:
        """Get color code for air quality level"""
        colors = {
            "good": "#00E400",
            "moderate": "#FFFF00",
            "unhealthy_sensitive": "#FF7E00",
            "unhealthy": "#FF0000",
            "very_unhealthy": "#8F3F97",
            "hazardous": "#7E0023",
            "comfortable": "#00E400",
            "uncomfortable": "#FF7E00",
            "dangerous": "#FF0000",
            "unknown": "#666666"
        }
        return colors.get(level, "#666666")
    
    def assess_overall_air_quality(self, measurements: List[Dict]) -> Dict:
        """Assess overall air quality based on available parameters"""
        if not measurements:
            return self._get_unknown_assessment("No measurements available")
        
        assessments = []
        available_parameters = set()
        
        # STEP 1: Assess each available parameter
        for measurement in measurements:
            param_name = measurement["parameter_name"]
            value = measurement["value"]
            
            assessment = self.get_air_quality_level(param_name, value)
            assessments.append({
                "parameter": measurement["parameter_name"],
                "value": value,
                "units": measurement["parameter_units"],
                "assessment": assessment
            })
            available_parameters.add(param_name.lower())
        
        # STEP 2: Determine overall quality using priority system
        overall_result = self._calculate_overall_quality(assessments, available_parameters)
        
        # STEP 3: Generate context-aware recommendations
        recommendations = self._generate_recommendations(
            overall_result["level"], 
            assessments, 
            available_parameters
        )
        
        return {
            "overall_quality": overall_result["level"],
            "message": overall_result["message"],
            "color": overall_result["color"],
            "primary_concern": overall_result["primary_concern"],
            "recommendations": recommendations,
            "data_confidence": self._calculate_confidence(available_parameters),
            "available_parameters": list(available_parameters),
            "detailed_assessments": assessments
        }
    
    def _calculate_overall_quality(self, assessments: List[Dict], available_params: set) -> Dict:
        """Calculate overall air quality with smart logic"""
        # Priority order (worst to best)
        priority_order = [
            "hazardous", "very_unhealthy", "unhealthy", 
            "unhealthy_sensitive", "uncomfortable", "dangerous",
            "moderate", "good", "comfortable"
        ]
        
        worst_level = "good"  # Start optimistic
        primary_concern = None
        
        # First pass: Find worst level among available parameters
        for assessment in assessments:
            level = assessment["assessment"]["level"]
            if level in priority_order and priority_order.index(level) < priority_order.index(worst_level):
                worst_level = level
                primary_concern = assessment["parameter"]
        
        # Special case: If we only have temperature/humidity but no air quality parameters
        has_air_quality_params = any(param in available_params for param in ["pm25", "pm10", "pm1", "um003"])
        has_comfort_params = any(param in available_params for param in ["temperature", "relativehumidity"])
        
        if not has_air_quality_params and has_comfort_params:
            # We can only assess comfort, not air quality
            comfort_levels = [a["assessment"]["level"] for a in assessments 
                            if a["parameter"].lower() in ["temperature", "relativehumidity"]]
            
            if any(level in ["uncomfortable", "dangerous"] for level in comfort_levels):
                worst_level = "moderate"  # Default to moderate if comfort is poor
                message = "Comfort conditions are poor, but air quality data is limited"
            else:
                worst_level = "good"
                message = "Comfort conditions are good, but air quality data is limited"
            
            return {
                "level": worst_level,
                "message": message,
                "color": self._get_color_code(worst_level),
                "primary_concern": primary_concern
            }
        
        # Normal case with air quality parameters
        return {
            "level": worst_level,
            "message": self.health_messages.get(worst_level, "Air quality assessment completed"),
            "color": self._get_color_code(worst_level),
            "primary_concern": primary_concern
        }
    
    def _calculate_confidence(self, available_params: set) -> str:
        """Calculate confidence level based on available parameters"""
        critical_params = ["pm25", "pm10"]
        secondary_params = ["pm1", "um003", "temperature", "relativehumidity"]
        
        critical_count = sum(1 for param in critical_params if param in available_params)
        secondary_count = sum(1 for param in secondary_params if param in available_params)
        
        if critical_count >= 2:
            return "high"
        elif critical_count >= 1:
            return "medium"
        elif secondary_count >= 2:
            return "low"
        else:
            return "very_low"
    
    def _generate_recommendations(self, overall_level: str, assessments: List[Dict], available_params: set) -> List[str]:
        """Generate smart recommendations based on available data"""
        recommendations = []
        
        # Base recommendations
        base_recommendations = {
            "good": [
                "Ideal conditions for outdoor activities",
                "Good time for opening windows for ventilation",
                "No special precautions needed"
            ],
            "moderate": [
                "Sensitive individuals should consider reducing prolonged outdoor exertion",
                "Generally acceptable for most activities",
                "Monitor conditions if you have respiratory issues"
            ],
            "unhealthy_sensitive": [
                "Sensitive groups should reduce outdoor activities",
                "People with heart or lung disease, older adults, and children should limit exertion",
                "Consider wearing a mask if outdoors for extended periods"
            ],
            "unhealthy": [
                "Everyone should reduce outdoor activities",
                "Avoid prolonged exertion",
                "Sensitive groups should avoid outdoor activities",
                "Keep windows closed and use air purifiers"
            ],
            "very_unhealthy": [
                "Avoid all outdoor activities",
                "Stay indoors with windows closed",
                "Use air purifiers if available",
                "Sensitive groups should take extra precautions"
            ],
            "hazardous": [
                "Emergency conditions - avoid all outdoor exposure",
                "Stay indoors with windows closed and air purification",
                "Consider relocating if conditions persist",
                "Follow local health authority guidance"
            ]
        }
        
        recommendations.extend(base_recommendations.get(overall_level, []))
        
        # Parameter-specific recommendations
        for assessment in assessments:
            level = assessment["assessment"]["level"]
            param = assessment["parameter"].lower()
            
            if level in ["unhealthy", "very_unhealthy", "hazardous"]:
                if "pm" in param:
                    recommendations.append(f"High {param} levels - consider using N95 masks outdoors")
                elif param == "temperature" and assessment["value"] > 35:
                    recommendations.append("Extreme heat - stay hydrated and avoid direct sun exposure")
                elif param == "relativehumidity" and assessment["value"] > 80:
                    recommendations.append("High humidity - may feel uncomfortable, use dehumidifiers if available")
        
        # Data limitation warnings
        if "pm25" not in available_params and "pm10" not in available_params:
            recommendations.append("Note: Limited air quality data available - assessment may be incomplete")
        
        return recommendations[:5]  # Limit to 5 most important
    
    def _get_unknown_assessment(self, reason: str) -> Dict:
        """Return assessment for unknown/no data cases"""
        return {
            "overall_quality": "unknown",
            "message": f"Cannot assess air quality: {reason}",
            "color": "#666666",
            "primary_concern": None,
            "recommendations": ["Check back later when more sensor data is available"],
            "data_confidence": "none",
            "available_parameters": [],
            "detailed_assessments": []
        }