from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
import json

class TransformJSON(FlowFileTransform):
 
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        name = "TransformJSON"
        version = "1.0"
        description = "TransformJSON"
        tags = ["test"]

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowfile):
        content = flowfile.getContentsAsBytes().decode("utf-8")
        data = json.loads(content)

        products = data.get("products", [])
        all_products = []

        
        for p in products:
            if p["rating"] >= 3:
                    rating = p.get("rating")

                    if rating >= 5:
                        label = "excellent"
                    elif rating >= 4:
                        label = "very good"
                    else:
                        label = "good"
                    
                    price = p.get("price")
                    discount = p.get("discountPercentage")
                    final_price = round(price - (price * discount / 100), 2)

                    all_products.append({
                        "title": p.get("title"),
                        "category": p.get("category"),
                        "rating": p.get("rating"),
                        "final_price": final_price,
                        "rating_label": label})
        
        return FlowFileTransformResult(
            relationship="success",
            contents=json.dumps(all_products),
            attributes={}
        )